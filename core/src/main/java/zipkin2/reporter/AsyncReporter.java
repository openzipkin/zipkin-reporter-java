/*
 * Copyright 2016-2018 The OpenZipkin Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package zipkin2.reporter;

import java.io.Flushable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;
import zipkin2.Call;
import zipkin2.CheckResult;
import zipkin2.Component;
import zipkin2.Span;
import zipkin2.codec.BytesEncoder;
import zipkin2.codec.SpanBytesEncoder;

import static java.lang.String.format;
import static java.util.logging.Level.FINE;

/**
 * As spans are reported, they are encoded and added to a pending queue. The task of sending spans
 * happens on a separate thread which calls {@link #flush()}. By doing so, callers are protected
 * from latency or exceptions possible when exporting spans out of process.
 *
 * <p>Spans are bundled into messages based on size in bytes or a timeout, whichever happens first.
 *
 * <p>The thread that sends flushes spans to the {@linkplain Sender} does so in a synchronous loop.
 * This means that even asynchronous transports will wait for an ack before sending a next message.
 * We do this so that a surge of spans doesn't overrun memory or bandwidth via hundreds or
 * thousands of in-flight messages. The downside of this is that reporting is limited in speed to
 * what a single thread can clear. When a thread cannot clear the backlog, new spans are dropped.
 *
 * @param <S> type of the span, usually {@link zipkin2.Span}
 */
public abstract class AsyncReporter<S> extends Component implements Reporter<S>, Flushable {
  /**
   * Builds a json reporter for <a href="http://zipkin.io/zipkin-api/#/">Zipkin V2</a>. If http,
   * the endpoint of the sender is usually "http://zipkinhost:9411/api/v2/spans".
   *
   * <p>After a certain threshold, spans are drained and {@link Sender#sendSpans(List) sent} to
   * Zipkin collectors.
   */
  public static AsyncReporter<zipkin2.Span> create(Sender sender) {
    return new Builder(sender).build();
  }

  /** Like {@link #create(Sender)}, except you can configure settings such as the timeout. */
  public static Builder builder(Sender sender) {
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

  public static final class Builder {
    final Sender sender;
    ReporterMetrics metrics = ReporterMetrics.NOOP_METRICS;
    int messageMaxBytes;
    long messageTimeoutNanos = TimeUnit.SECONDS.toNanos(1);
    long closeTimeoutNanos = TimeUnit.SECONDS.toNanos(1);
    int queuedMaxSpans = 10000;
    int queuedMaxBytes = onePercentOfMemory();

    static int onePercentOfMemory() {
      long result = (long) (Runtime.getRuntime().totalMemory() * 0.01);
      // don't overflow in the rare case 1% of memory is larger than 2 GiB!
      return (int) Math.max(Math.min(Integer.MAX_VALUE, result), Integer.MIN_VALUE);
    }

    Builder(Sender sender) {
      if (sender == null) throw new NullPointerException("sender == null");
      this.sender = sender;
      this.messageMaxBytes = sender.messageMaxBytes();
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
     * Sender#messageMaxBytes()}.
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
     * Sender#messageMaxBytes()}. This timeout ensures that spans are not stuck in an incomplete
     * message.
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

    /** Builds an async reporter that encodes zipkin spans as they are reported. */
    public AsyncReporter<Span> build() {
      switch (sender.encoding()) {
        case JSON:
          return build(SpanBytesEncoder.JSON_V2);
        case PROTO3:
          return build(SpanBytesEncoder.PROTO3);
        case THRIFT:
          return build(SpanBytesEncoder.THRIFT);
        default:
          throw new UnsupportedOperationException(sender.encoding().name());
      }
    }

    /** Builds an async reporter that encodes arbitrary spans as they are reported. */
    public <S> AsyncReporter<S> build(BytesEncoder<S> encoder) {
      if (encoder == null) throw new NullPointerException("encoder == null");

      if (encoder.encoding() != sender.encoding()) {
        throw new IllegalArgumentException(String.format(
            "Encoder doesn't match Sender: %s %s", encoder.encoding(), sender.encoding()));
      }

      final BoundedAsyncReporter<S> result = new BoundedAsyncReporter<>(this, encoder);

      if (messageTimeoutNanos > 0) { // Start a thread that flushes the queue in a loop.
        final BufferNextMessage<S> consumer =
            BufferNextMessage.create(encoder.encoding(), messageMaxBytes, messageTimeoutNanos);
        final Thread flushThread = new Thread("AsyncReporter{" + sender + "}") {
          @Override public void run() {
            try {
              while (!result.closed.get()) {
                result.flush(consumer);
              }
            } catch (RuntimeException | Error e) {
              BoundedAsyncReporter.logger.log(Level.WARNING, "Unexpected error flushing spans", e);
              throw e;
            } finally {
              int count = consumer.count();
              if (count > 0) {
                metrics.incrementSpansDropped(count);
                BoundedAsyncReporter.logger.warning("Dropped " + count + " spans due to AsyncReporter.close()");
              }
              result.close.countDown();
            }
          }
        };
        flushThread.setDaemon(true);
        flushThread.start();
      }
      return result;
    }
  }

  static final class BoundedAsyncReporter<S> extends AsyncReporter<S> {
    static final Logger logger = Logger.getLogger(BoundedAsyncReporter.class.getName());
    final AtomicBoolean closed = new AtomicBoolean(false);
    final BytesEncoder<S> encoder;
    final ByteBoundedQueue<S> pending;
    final Sender sender;
    final int messageMaxBytes;
    final long messageTimeoutNanos;
    final long closeTimeoutNanos;
    final CountDownLatch close;
    final ReporterMetrics metrics;

    BoundedAsyncReporter(Builder builder, BytesEncoder<S> encoder) {
      this.pending = new ByteBoundedQueue<>(builder.queuedMaxSpans, builder.queuedMaxBytes);
      this.sender = builder.sender;
      this.messageMaxBytes = builder.messageMaxBytes;
      this.messageTimeoutNanos = builder.messageTimeoutNanos;
      this.closeTimeoutNanos = builder.closeTimeoutNanos;
      this.close = new CountDownLatch(builder.messageTimeoutNanos > 0 ? 1 : 0);
      this.metrics = builder.metrics;
      this.encoder = encoder;
    }

    /** Returns true if the was encoded and accepted onto the queue. */
    @Override public void report(S next) {
      if (next == null) throw new NullPointerException("span == null");
      metrics.incrementSpans(1);
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

    @Override public final void flush() {
      flush(BufferNextMessage.create(encoder.encoding(), messageMaxBytes, 0));
    }

    void flush(BufferNextMessage<S> bundler) {
      if (closed.get()) throw new IllegalStateException("closed");

      pending.drainTo(bundler, bundler.remainingNanos());

      // record after flushing reduces the amount of gauge events vs on doing this on report
      metrics.updateQueuedSpans(pending.count);
      metrics.updateQueuedBytes(pending.sizeInBytes);

      // loop around if we are running, and the bundle isn't full
      // if we are closed, try to send what's pending
      if (!bundler.isReady() && !closed.get()) return;

      // Signal that we are about to send a message of a known size in bytes
      metrics.incrementMessages();
      metrics.incrementMessageBytes(bundler.sizeInBytes());

      // Create the next message. Since we are outside the lock shared with writers, we can encode
      ArrayList<byte[]> nextMessage = new ArrayList<>(bundler.count());
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
        sender.sendSpans(nextMessage).execute();
      } catch (IOException | RuntimeException | Error t) {
        // In failure case, we increment messages and spans dropped.
        int count = nextMessage.size();
        Call.propagateIfFatal(t);
        metrics.incrementMessagesDropped(t);
        metrics.incrementSpansDropped(count);
        if (logger.isLoggable(FINE)) {
          logger.log(FINE,
              format("Dropped %s spans due to %s(%s)", count, t.getClass().getSimpleName(),
                  t.getMessage() == null ? "" : t.getMessage()), t);
        }
        // Raise in case the sender was closed out-of-band.
        if (t instanceof IllegalStateException) throw (IllegalStateException) t;
      }
    }

    @Override public CheckResult check() {
      return sender.check();
    }

    @Override public void close() {
      if (!closed.compareAndSet(false, true)) return; // already closed
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

    @Override public String toString() {
      return "AsyncReporter{" + sender + "}";
    }
  }
}
