/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin2.reporter.internal;

import java.io.Closeable;
import java.io.Flushable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;
import zipkin2.reporter.BytesEncoder;
import zipkin2.reporter.BytesMessageSender;
import zipkin2.reporter.Call;
import zipkin2.reporter.CheckResult;
import zipkin2.reporter.ClosedSenderException;
import zipkin2.reporter.Component;
import zipkin2.reporter.Reporter;
import zipkin2.reporter.ReporterMetrics;

import static java.lang.String.format;
import static java.util.logging.Level.FINE;
import static java.util.logging.Level.WARNING;

/**
 * As spans are reported, they are encoded and added to a pending queue. The task of sending spans
 * happens on a separate thread which calls {@link #flush()}. By doing so, callers are protected
 * from latency or exceptions possible when exporting spans out of process.
 *
 * <p>Spans are bundled into messages based on size in bytes or a timeout, whichever happens first.
 *
 * <p>The thread that sends flushes spans to the {@linkplain BytesMessageSender} does so in a
 * synchronous loop. This means that even asynchronous transports will wait for an ack before
 * sending a next message. We do this so that a surge of spans doesn't overrun memory or bandwidth
 * via hundreds or thousands of in-flight messages. The downside of this is that reporting is
 * limited in speed to what a single thread can clear. When a thread cannot clear the backlog, new
 * spans are dropped.
 *
 * @param <S> type of the span, usually {@code zipkin2.Span}
 * @since 3.0
 */
public abstract class AsyncReporter<S> extends Component implements Reporter<S>, Closeable, Flushable {
  public static Builder newBuilder(BytesMessageSender sender) {
    return new Builder(sender);
  }

  /**
   * Calling this will flush any pending spans to the transport on the current thread.
   *
   * <p>Note: If you set {@link Builder#messageTimeout(long, TimeUnit) message timeout} to zero, you
   * must call this externally as otherwise spans will never be sent.
   *
   * @throws IllegalStateException if closed
   */
  @Override public abstract void flush();

  /** Shuts down the sender thread, and increments drop metrics if there were any unsent spans. */
  @Override public abstract void close();

  public abstract Builder toBuilder();

  public static final class Builder {
    final BytesMessageSender sender;
    ThreadFactory threadFactory = Executors.defaultThreadFactory();
    ReporterMetrics metrics = ReporterMetrics.NOOP_METRICS;
    int messageMaxBytes;
    long messageTimeoutNanos = TimeUnit.SECONDS.toNanos(1);
    long closeTimeoutNanos = TimeUnit.SECONDS.toNanos(1);
    int queuedMaxSpans = 10000;
    int queuedMaxBytes = onePercentOfMemory();

    Builder(BoundedAsyncReporter<?> asyncReporter) {
      this.sender = asyncReporter.sender;
      this.threadFactory = asyncReporter.threadFactory;
      this.metrics = asyncReporter.metrics;
      this.messageMaxBytes = asyncReporter.messageMaxBytes;
      this.messageTimeoutNanos = asyncReporter.messageTimeoutNanos;
      this.closeTimeoutNanos = asyncReporter.closeTimeoutNanos;
      this.queuedMaxSpans = asyncReporter.pending.maxSize();
      this.queuedMaxBytes = asyncReporter.pending.maxBytes();
    }

    static int onePercentOfMemory() {
      long result = (long) (Runtime.getRuntime().totalMemory() * 0.01);
      // don't overflow in the rare case 1% of memory is larger than 2 GiB!
      return (int) Math.max(Math.min(Integer.MAX_VALUE, result), Integer.MIN_VALUE);
    }

    Builder(BytesMessageSender sender) {
      if (sender == null) throw new NullPointerException("sender == null");
      this.sender = sender;
      this.messageMaxBytes = sender.messageMaxBytes();
    }

    /**
     * Launches the flush thread when {@link #messageTimeoutNanos} is greater than zero.
     */
    public Builder threadFactory(ThreadFactory threadFactory) {
      if (threadFactory == null) throw new NullPointerException("threadFactory == null");
      this.threadFactory = threadFactory;
      return this;
    }

    /**
     * Aggregates and reports reporter metrics to a monitoring system. Defaults to no-op.
     */
    public Builder metrics(ReporterMetrics metrics) {
      if (metrics == null) throw new NullPointerException("metrics == null");
      this.metrics = metrics;
      return this;
    }

    /**
     * Maximum bytes sendable per message including overhead. Defaults to, and is limited by {@link
     * BytesMessageSender#messageMaxBytes()}.
     */
    public Builder messageMaxBytes(int messageMaxBytes) {
      if (messageMaxBytes < 0) {
        throw new IllegalArgumentException("messageMaxBytes < 0: " + messageMaxBytes);
      }
      this.messageMaxBytes = Math.min(messageMaxBytes, sender.messageMaxBytes());
      return this;
    }

    /**
     * Default 1 second. 0 implies spans are {@link #flush() flushed} externally.
     *
     * <p>Instead of sending one message at a time, spans are bundled into messages, up to {@link
     * BytesMessageSender#messageMaxBytes()}. This timeout ensures that spans are not stuck in an
     * incomplete message.
     *
     * <p>Note: this timeout starts when the first unsent span is reported.
     */
    public Builder messageTimeout(long timeout, TimeUnit unit) {
      if (timeout < 0) throw new IllegalArgumentException("messageTimeout < 0: " + timeout);
      if (unit == null) throw new NullPointerException("unit == null");
      this.messageTimeoutNanos = unit.toNanos(timeout);
      return this;
    }

    /** How long to block for in-flight spans to send out-of-process on close. Default 1 second */
    public Builder closeTimeout(long timeout, TimeUnit unit) {
      if (timeout < 0) throw new IllegalArgumentException("closeTimeout < 0: " + timeout);
      if (unit == null) throw new NullPointerException("unit == null");
      this.closeTimeoutNanos = unit.toNanos(timeout);
      return this;
    }

    /** Maximum backlog of spans reported vs sent. Default 10000 */
    public Builder queuedMaxSpans(int queuedMaxSpans) {
      this.queuedMaxSpans = queuedMaxSpans;
      return this;
    }

    /** Maximum backlog of span bytes reported vs sent. Default 1% of heap */
    public Builder queuedMaxBytes(int queuedMaxBytes) {
      this.queuedMaxBytes = queuedMaxBytes;
      return this;
    }

    /** Builds an async reporter that encodes arbitrary spans as they are reported. */
    public <S> AsyncReporter<S> build(BytesEncoder<S> encoder) {
      if (encoder == null) throw new NullPointerException("encoder == null");

      if (encoder.encoding() != sender.encoding()) {
        throw new IllegalArgumentException(String.format(
          "Encoder doesn't match Sender: %s %s", encoder.encoding(), sender.encoding()));
      }

      return new BoundedAsyncReporter<S>(this, encoder);
    }
  }

  static final class BoundedAsyncReporter<S> extends AsyncReporter<S> {
    static final Logger logger = Logger.getLogger(BoundedAsyncReporter.class.getName());
    final AtomicBoolean started, closed;
    final BytesEncoder<S> encoder;
    final BoundedQueue<S> pending;
    final BytesMessageSender sender;
    final int messageMaxBytes;
    final long messageTimeoutNanos, closeTimeoutNanos;
    final CountDownLatch close;
    final ReporterMetrics metrics;
    final ThreadFactory threadFactory;

    /** Tracks if we should log the first instance of an exception in flush(). */
    private boolean shouldWarnException = true;

    BoundedAsyncReporter(Builder builder, BytesEncoder<S> encoder) {
      this.pending = BoundedQueue.create(builder.queuedMaxSpans, builder.queuedMaxBytes);
      this.sender = builder.sender;
      this.messageMaxBytes = builder.messageMaxBytes;
      this.messageTimeoutNanos = builder.messageTimeoutNanos;
      this.closeTimeoutNanos = builder.closeTimeoutNanos;
      this.closed = new AtomicBoolean(false);
      // pretend we already started when config implies no thread that flushes the queue in a loop.
      this.started = new AtomicBoolean(builder.messageTimeoutNanos == 0);
      this.close = new CountDownLatch(builder.messageTimeoutNanos > 0 ? 1 : 0);
      this.metrics = builder.metrics;
      this.threadFactory = builder.threadFactory;
      this.encoder = encoder;
    }

    void startFlusherThread() {
      BufferNextMessage<S> consumer =
        BufferNextMessage.create(encoder.encoding(), messageMaxBytes, messageTimeoutNanos);
      Thread flushThread = threadFactory.newThread(new Flusher<S>(this, consumer));
      flushThread.setName("AsyncReporter{" + sender + "}");
      flushThread.setDaemon(true);
      flushThread.start();
    }

    @SuppressWarnings("unchecked")
    @Override public void report(S next) {
      if (next == null) throw new NullPointerException("span == null");
      // Lazy start so that reporters never used don't spawn threads
      if (started.compareAndSet(false, true)) startFlusherThread();
      metrics.incrementSpans(1);
      
      if (pending instanceof UnsizedSpanConsumer) {
          // enqueue now and filter our when we drain
        final UnsizedSpanConsumer<S> consumer = (UnsizedSpanConsumer<S>)pending;
        if (closed.get() || !consumer.offer(next)) {
          metrics.incrementSpansDropped(1);
        }
      } else {
        int nextSizeInBytes = encoder.sizeInBytes(next);
        int messageSizeOfNextSpan = sender.messageSizeInBytes(nextSizeInBytes);
        metrics.incrementSpanBytes(nextSizeInBytes);
        if (closed.get() ||
          // don't enqueue something larger than we can drain
          messageSizeOfNextSpan > messageMaxBytes ||
          !pending.offer(next, nextSizeInBytes)) {
          metrics.incrementSpansDropped(1);
        }
      } 
    }

    @Override public void flush() {
      if (closed.get()) throw new ClosedSenderException();
      flush(BufferNextMessage.<S>create(encoder.encoding(), messageMaxBytes, 0));
    }

    void flush(BufferNextMessage<S> bundler) {
      pending.drainTo(bundler, bundler.remainingNanos());

      // record after flushing reduces the amount of gauge events vs on doing this on report
      metrics.updateQueuedSpans(pending.count());
      metrics.updateQueuedBytes(pending.sizeInBytes());

      // loop around if we are running, and the bundle isn't full
      // if we are closed, try to send what's pending
      if (!bundler.isReady() && !closed.get()) return;

      // Signal that we are about to send a message of a known size in bytes
      metrics.incrementMessages();
      metrics.incrementMessageBytes(bundler.sizeInBytes());

      // Create the next message. Since we are outside the lock shared with writers, we can encode
      final ArrayList<byte[]> nextMessage = new ArrayList<byte[]>(bundler.count());
      bundler.drain(new SpanWithSizeConsumer<S>() {
        @Override public boolean offer(S next, int nextSizeInBytes) {
          nextMessage.add(encoder.encode(next)); // speculatively add to the pending message
          if (sender.messageSizeInBytes(nextMessage) > messageMaxBytes) {
            // if we overran the message size, remove the encoded message.
            nextMessage.remove(nextMessage.size() - 1);
            return false;
          }
          return true;
        }
      });

      try {
        sender.send(nextMessage);
      } catch (Throwable t) {
        // In failure case, we increment messages and spans dropped.
        int count = nextMessage.size();
        Call.propagateIfFatal(t);
        metrics.incrementMessagesDropped(t);
        metrics.incrementSpansDropped(count);

        Level logLevel = FINE;

        if (shouldWarnException) {
          logger.log(WARNING, "Spans were dropped due to exceptions. "
            + "All subsequent errors will be logged at FINE level.");
          logLevel = WARNING;
          shouldWarnException = false;
        }

        if (logger.isLoggable(logLevel)) {
          logger.log(logLevel,
            format("Dropped %s spans due to %s(%s)", count, t.getClass().getSimpleName(),
              t.getMessage() == null ? "" : t.getMessage()), t);
        }

        // Raise in case the sender was closed out-of-band.
        if (t instanceof ClosedSenderException) throw (ClosedSenderException) t;

        // Old senders in other artifacts may be using this less precise way of indicating they've been closed
        // out-of-band.
        if (t instanceof IllegalStateException && t.getMessage().equals("closed")) {
          throw (IllegalStateException) t;
        }
      }
    }

    @Override @Deprecated public CheckResult check() {
      try {
        sender.send(Collections.<byte[]>emptyList());
        return CheckResult.OK;
      } catch (Throwable t) {
        Call.propagateIfFatal(t);
        return CheckResult.failed(t);
      }
    }

    @Override public void close() {
      if (!closed.compareAndSet(false, true)) return; // already closed
      started.set(true); // prevent anything from starting the thread after close!
      try {
        // wait for in-flight spans to send
        if (!close.await(closeTimeoutNanos, TimeUnit.NANOSECONDS)) {
          logger.warning("Timed out waiting for in-flight spans to send");
        }
      } catch (InterruptedException e) {
        logger.warning("Interrupted waiting for in-flight spans to send");
        Thread.currentThread().interrupt();
      }
      int count = pending.clear();
      if (count > 0) {
        metrics.incrementSpansDropped(count);
        logger.warning("Dropped " + count + " spans due to AsyncReporter.close()");
      }
    }

    @Override public Builder toBuilder() {
      return new Builder(this);
    }

    @Override public String toString() {
      return "AsyncReporter{" + sender + "}";
    }
  }

  static final class Flusher<S> implements Runnable {
    static final Logger logger = Logger.getLogger(Flusher.class.getName());

    final BoundedAsyncReporter<S> result;
    final BufferNextMessage<S> consumer;

    Flusher(BoundedAsyncReporter<S> result, BufferNextMessage<S> consumer) {
      this.result = result;
      this.consumer = consumer;
    }

    @Override public void run() {
      try {
        while (!result.closed.get()) {
          result.flush(consumer);
        }
      } catch (RuntimeException e) {
        logger.log(Level.WARNING, "Unexpected error flushing spans", e);
        throw e;
      } catch (Error e) {
        logger.log(Level.WARNING, "Unexpected error flushing spans", e);
        throw e;
      } finally {
        int count = consumer.count();
        if (count > 0) {
          result.metrics.incrementSpansDropped(count);
          logger.warning("Dropped " + count + " spans due to AsyncReporter.close()");
        }
        result.close.countDown();
      }
    }

    @Override public String toString() {
      return "AsyncReporter{" + result.sender + "}";
    }
  }
}
