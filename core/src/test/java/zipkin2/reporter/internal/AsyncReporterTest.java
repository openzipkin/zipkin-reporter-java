/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin2.reporter.internal;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import zipkin2.Span;
import zipkin2.TestObjects;
import zipkin2.reporter.BytesEncoder;
import zipkin2.reporter.ClosedSenderException;
import zipkin2.reporter.Encoding;
import zipkin2.reporter.FakeSender;
import zipkin2.reporter.InMemoryReporterMetrics;
import zipkin2.reporter.SpanBytesEncoder;
import zipkin2.reporter.internal.AsyncReporter.BoundedAsyncReporter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.failBecauseExceptionWasNotThrown;
import static org.junit.jupiter.api.Assertions.assertThrows;

class AsyncReporterTest {

  Span span = TestObjects.CLIENT_SPAN;
  int sizeInBytesOfSingleSpanMessage =
    Encoding.JSON.listSizeInBytes(
      Collections.singletonList(SpanBytesEncoder.JSON_V2.encode(span)));

  InMemoryReporterMetrics metrics = new InMemoryReporterMetrics();

  @ParameterizedTest(name = "queuedMaxBytes={0}")
  @ValueSource(ints = { 0, 1000000 })
  void messageMaxBytes_defaultsToSender(int queuedMaxBytes) {
    AtomicInteger sentSpans = new AtomicInteger();
    AsyncReporter<Span> reporter = AsyncReporter.newBuilder(FakeSender.create()
        .onSpans(spans -> sentSpans.addAndGet(spans.size()))
      .messageMaxBytes(sizeInBytesOfSingleSpanMessage))
      .queuedMaxBytes(queuedMaxBytes)
      .messageTimeout(0, TimeUnit.MILLISECONDS)
      .build(SpanBytesEncoder.JSON_V2);

    reporter.report(span);
    reporter.report(span); // drops
    reporter.flush();
    reporter.close();

    assertThat(sentSpans.get()).isEqualTo(1);
  }

  @ParameterizedTest(name = "queuedMaxBytes={0}")
  @ValueSource(ints = { 0, 1000000 })
  void messageMaxBytes_dropsWhenOverqueuing(int queuedMaxBytes) {
    AtomicInteger sentSpans = new AtomicInteger();
    AsyncReporter<Span> reporter = AsyncReporter.newBuilder(FakeSender.create()
        .onSpans(spans -> sentSpans.addAndGet(spans.size())))
      .messageMaxBytes(sizeInBytesOfSingleSpanMessage)
      .queuedMaxBytes(queuedMaxBytes)
      .messageTimeout(0, TimeUnit.MILLISECONDS)
      .build(SpanBytesEncoder.JSON_V2);

    reporter.report(span);
    reporter.report(span); // dropped the one that queued more than allowed bytes
    reporter.flush();
    reporter.close();

    assertThat(sentSpans.get()).isEqualTo(1);
  }

  @ParameterizedTest(name = "queuedMaxBytes={0}")
  @ValueSource(ints = { 0, 1000000 })
  void messageMaxBytes_dropsWhenTooLarge(int queuedMaxBytes) {
    AtomicInteger sentSpans = new AtomicInteger();
    AsyncReporter<Span> reporter = AsyncReporter.newBuilder(FakeSender.create()
        .onSpans(spans -> sentSpans.addAndGet(spans.size())))
      .messageMaxBytes(sizeInBytesOfSingleSpanMessage)
      .queuedMaxBytes(queuedMaxBytes)
      .messageTimeout(0, TimeUnit.MILLISECONDS)
      .build(SpanBytesEncoder.JSON_V2);

    reporter.report(span.toBuilder().addAnnotation(1L, "fooooo").build());
    reporter.flush();
    reporter.close();

    assertThat(sentSpans.get()).isEqualTo(0);
  }

  @ParameterizedTest(name = "queuedMaxBytes={0}")
  @ValueSource(ints = { 0, 1000000 })
  void queuedMaxSpans_dropsWhenOverqueuing(int queuedMaxBytes) {
    AtomicInteger sentSpans = new AtomicInteger();
    AsyncReporter<Span> reporter = AsyncReporter.newBuilder(FakeSender.create()
        .onSpans(spans -> sentSpans.addAndGet(spans.size())))
      .queuedMaxSpans(1)
      .queuedMaxBytes(queuedMaxBytes)
      .messageTimeout(0, TimeUnit.MILLISECONDS)
      .build(SpanBytesEncoder.JSON_V2);

    reporter.report(span);
    reporter.report(span); // dropped the one that queued more than allowed count
    reporter.flush();
    reporter.close();
    
    assertThat(sentSpans.get()).isEqualTo(1);
  }

  @ParameterizedTest(name = "queuedMaxBytes={0}")
  @ValueSource(ints = { 0, 1000000 })
  void report_incrementsMetrics(int queuedMaxBytes) {
    AsyncReporter<Span> reporter = AsyncReporter.newBuilder(FakeSender.create())
      .metrics(metrics)
      .queuedMaxBytes(queuedMaxBytes)
      .messageTimeout(0, TimeUnit.MILLISECONDS)
      .build(SpanBytesEncoder.JSON_V2);

    reporter.report(span);
    reporter.report(span);
    reporter.flush();
    reporter.close();
    
    assertThat(metrics.spans()).isEqualTo(2);
    assertThat(metrics.spanBytes()).isEqualTo(SpanBytesEncoder.JSON_V2.encode(span).length * 2);
  }

  @ParameterizedTest(name = "queuedMaxBytes={0}")
  @ValueSource(ints = { 0, 1000000 })
  void report_incrementsSpansDropped(int queuedMaxBytes) {
    AsyncReporter<Span> reporter = AsyncReporter.newBuilder(FakeSender.create())
      .queuedMaxSpans(1)
      .metrics(metrics)
      .queuedMaxBytes(queuedMaxBytes)
      .messageTimeout(0, TimeUnit.MILLISECONDS)
      .build(SpanBytesEncoder.JSON_V2);

    reporter.report(span);
    reporter.report(span);
    reporter.flush();
    reporter.close();

    assertThat(metrics.spans()).isEqualTo(2);
    assertThat(metrics.spansDropped()).isEqualTo(1);
  }

  @ParameterizedTest(name = "queuedMaxBytes={0}")
  @ValueSource(ints = { 0, 1000000 })
  void flush_incrementsMetrics(int queuedMaxBytes) {
    AsyncReporter<Span> reporter = AsyncReporter.newBuilder(FakeSender.create())
      .metrics(metrics)
      .messageMaxBytes(sizeInBytesOfSingleSpanMessage)
      .queuedMaxBytes(queuedMaxBytes)
      .messageTimeout(0, TimeUnit.MILLISECONDS)
      .build(SpanBytesEncoder.JSON_V2);

    // Queue up 2 spans
    reporter.report(span);
    reporter.report(span);

    reporter.flush();
    assertThat(metrics.queuedSpans()).isEqualTo(1); // still one span in the backlog
    assertThat(metrics.queuedBytes()).isEqualTo(queuedMaxBytes > 0 ? SpanBytesEncoder.JSON_V2.encode(span).length : 0);
    assertThat(metrics.messages()).isEqualTo(1);
    assertThat(metrics.messageBytes()).isEqualTo(sizeInBytesOfSingleSpanMessage);

    reporter.flush();
    reporter.close();
    assertThat(metrics.queuedSpans()).isZero();
    assertThat(metrics.queuedBytes()).isZero();
    assertThat(metrics.messages()).isEqualTo(2);
    assertThat(metrics.messageBytes()).isEqualTo(sizeInBytesOfSingleSpanMessage * 2);
  }

  @ParameterizedTest(name = "queuedMaxBytes={0}")
  @ValueSource(ints = { 0, 1000000 })
  void flush_incrementsMessagesDropped(int queuedMaxBytes) {
    AsyncReporter<Span> reporter = AsyncReporter.newBuilder(FakeSender.create()
        .onSpans(spans -> {
          throw new RuntimeException();
        }))
      .metrics(metrics)
      .queuedMaxBytes(queuedMaxBytes)
      .messageTimeout(0, TimeUnit.MILLISECONDS)
      .build(SpanBytesEncoder.JSON_V2);

    reporter.report(span);

    reporter.flush();
    reporter.close();
    assertThat(metrics.messagesDropped()).isEqualTo(1);
  }

  @ParameterizedTest(name = "queuedMaxBytes={0}")
  @ValueSource(ints = { 0, 1000000 })
  void flush_logsFirstErrorAsWarn(int queuedMaxBytes) {
    List<LogRecord> logRecords = new ArrayList<>();
    Handler testHandler = new Handler() {
      @Override
      public void publish(LogRecord record) {
        logRecords.add(record);
      }

      @Override
      public void flush() {
      }

      @Override
      public void close() throws SecurityException {
      }
    };

    Logger logger = Logger.getLogger(BoundedAsyncReporter.class.getName());
    logger.addHandler(testHandler);
    logger.setLevel(Level.FINE);

    AsyncReporter<Span> reporter = AsyncReporter.newBuilder(FakeSender.create()
        .onSpans(spans -> {
          throw new RuntimeException();
        }))
      .queuedMaxBytes(queuedMaxBytes)
      .build(SpanBytesEncoder.JSON_V2);

    reporter.report(span);
    reporter.flush();

    reporter.report(span);
    reporter.flush();

    assertThat(logRecords).hasSize(3);
    assertThat(logRecords.get(0).getLevel()).isEqualTo(Level.WARNING);

    assertThat(logRecords.get(1).getLevel()).isEqualTo(Level.WARNING);
    assertThat(logRecords.get(1).getMessage()).contains("RuntimeException");

    assertThat(logRecords.get(2).getLevel()).isEqualTo(Level.FINE);
    assertThat(logRecords.get(2).getMessage()).contains("RuntimeException");

    reporter.close();
  }

  /** It can take up to the messageTimeout past the first span to send */
  @ParameterizedTest(name = "queuedMaxBytes={0}")
  @ValueSource(ints = { 0, 1000000 })
  void messageTimeout_flushesWhenTimeoutExceeded(int queuedMaxBytes) throws InterruptedException {
    CountDownLatch sentSpans = new CountDownLatch(1);
    AsyncReporter<Span> reporter = AsyncReporter.newBuilder(FakeSender.create()
        .onSpans(spans -> sentSpans.countDown()))
      .queuedMaxBytes(queuedMaxBytes)
      .messageTimeout(10, TimeUnit.MILLISECONDS)
      .build(SpanBytesEncoder.JSON_V2);

    reporter.report(span);
    assertThat(sentSpans.await(5, TimeUnit.MILLISECONDS))
      .isFalse();
    assertThat(sentSpans.await(10, TimeUnit.MILLISECONDS))
      .isTrue();

    reporter.close();
  }

  @ParameterizedTest(name = "queuedMaxBytes={0}")
  @ValueSource(ints = { 0, 1000000 })
  void messageTimeout_disabled(int queuedMaxBytes) throws InterruptedException {
    CountDownLatch sentSpans = new CountDownLatch(1);
    AsyncReporter<Span> reporter = AsyncReporter.newBuilder(FakeSender.create()
        .onSpans(spans -> sentSpans.countDown()))
      .queuedMaxBytes(queuedMaxBytes)
      .messageTimeout(0, TimeUnit.NANOSECONDS)
      .build(SpanBytesEncoder.JSON_V2);

    reporter.report(span);
    assertThat(sentSpans.getCount()).isEqualTo(1);

    // Since no threads started, the above lingers
    assertThat(sentSpans.await(10, TimeUnit.MILLISECONDS))
      .isFalse();

    reporter.close();
  }

  @Test void senderThread_threadHasAPrettyName() throws InterruptedException {
    BlockingQueue<String> threadName = new LinkedBlockingQueue<>();
    AsyncReporter<Span> reporter = AsyncReporter.newBuilder(FakeSender.create()
        .onSpans(spans -> threadName.offer(Thread.currentThread().getName())))
      .build(SpanBytesEncoder.JSON_V2);

    reporter.report(span);

    // check name is pretty
    assertThat(threadName.take())
      .isEqualTo("AsyncReporter{FakeSender}");
    
    reporter.close();
  }

  @Test void close_close_stopsFlushThread() throws InterruptedException {
    AsyncReporter<Span> reporter = AsyncReporter.newBuilder(FakeSender.create())
      .metrics(metrics)
      .messageTimeout(2, TimeUnit.MILLISECONDS)
      .build(SpanBytesEncoder.JSON_V2);

    // Reporter thread is lazy
    assertThat(((BoundedAsyncReporter) reporter).started).isFalse();
    reporter.report(span);
    assertThat(((BoundedAsyncReporter) reporter).started).isTrue();

    reporter.close();

    // the close latch counts down when the thread is stopped
    BoundedAsyncReporter<Span> impl = (BoundedAsyncReporter<Span>) reporter;
    assertThat(impl.close.await(3, TimeUnit.MILLISECONDS))
      .isTrue();
    
    reporter.close();
  }

  @Test void flush_throwsOnClose() {
    assertThrows(IllegalStateException.class, () -> {
      AsyncReporter<Span> reporter = AsyncReporter.newBuilder(FakeSender.create())
        .metrics(metrics)
        .messageTimeout(0, TimeUnit.MILLISECONDS)
        .build(SpanBytesEncoder.JSON_V2);

      reporter.report(span);
      reporter.close(); // close while there's a pending span
      reporter.flush();
    });
  }

  @Test void report_doesntThrowWhenClosed() {
    AsyncReporter<Span> reporter = AsyncReporter.newBuilder(FakeSender.create())
      .metrics(metrics)
      .messageTimeout(0, TimeUnit.MILLISECONDS)
      .build(SpanBytesEncoder.JSON_V2);

    reporter.close();

    // Don't throw, as that could crash apps. Just increment drop metrics
    reporter.report(span);
    assertThat(metrics.spansDropped()).isEqualTo(1);
  }

  FakeSender sleepingSender = FakeSender.create().onSpans(spans -> {
    try {
      Thread.sleep(5);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  });

  @Test void senderThread_dropsOnSenderClose_flushThread() throws InterruptedException {
    AsyncReporter<Span> reporter = AsyncReporter.newBuilder(sleepingSender)
      .metrics(metrics)
      .messageMaxBytes(sizeInBytesOfSingleSpanMessage)
      .build(SpanBytesEncoder.JSON_V2);

    reporter.report(span);
    Thread.sleep(1); // flush thread got the first span, but still waiting for more
    reporter.report(span);
    sleepingSender.close(); // close while there's a pending span
    Thread.sleep(10); // wait for the poll to unblock

    assertThat(metrics.spansDropped()).isEqualTo(1);
    assertThat(metrics.messagesDropped()).isEqualTo(1);
    assertThat(metrics.messagesDroppedByCause().keySet().iterator().next())
      .isEqualTo(ClosedSenderException.class);
    
    reporter.close();
  }

  @Test void senderThread_dropsOnReporterClose_flushThread() throws InterruptedException {
    CountDownLatch received = new CountDownLatch(1);
    CountDownLatch sent = new CountDownLatch(1);
    AsyncReporter<Span> reporter = AsyncReporter.newBuilder(FakeSender.create()
        .onSpans(spans -> {
          received.countDown();
          try {
            sent.await(); // block the flush thread
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        }))
      .metrics(metrics)
      .messageMaxBytes(sizeInBytesOfSingleSpanMessage)
      .build(SpanBytesEncoder.JSON_V2);

    reporter.report(span);
    reporter.report(span); // pending as the flush thread is blocked
    received.await(); // wait for the first span to send
    reporter.close(); // close while there's a pending span
    sent.countDown(); // release the flush thread

    assertThat(metrics.spansDropped()).isEqualTo(1);
  }

  @ParameterizedTest(name = "queuedMaxBytes={0}")
  @ValueSource(ints = { 0, 1000000 })
  void blocksToClearPendingSpans(int queuedMaxBytes) throws InterruptedException {
    AsyncReporter<Span> reporter = AsyncReporter.newBuilder(FakeSender.create())
      .metrics(metrics)
      .queuedMaxBytes(queuedMaxBytes)
      .messageTimeout(30, TimeUnit.SECONDS)
      .build(SpanBytesEncoder.JSON_V2);

    reporter.report(span);
    Thread.sleep(500); // wait for the thread to start

    reporter.close(); // close while there's a pending span

    assertThat(metrics.spans()).isEqualTo(1);
    assertThat(metrics.spansDropped()).isEqualTo(0);
  }

  @ParameterizedTest(name = "queuedMaxBytes={0}")
  @ValueSource(ints = { 0, 1000000 })
  void quitsBlockingWhenOverTimeout(int queuedMaxBytes) throws InterruptedException {
    AsyncReporter<Span> reporter = AsyncReporter.newBuilder(FakeSender.create()
        .onSpans(spans -> {
          // note: we don't yet have a hook to cancel a sender, so this will remain in-flight
          // eventhough we are unblocking close. A later close on sender usually will kill in-flight
          try {
            Thread.sleep(1000); // block the flush thread longer than closeTimeout
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        }))
      .metrics(metrics)
      .queuedMaxBytes(queuedMaxBytes)
      .closeTimeout(1, TimeUnit.NANOSECONDS)
      .messageTimeout(30, TimeUnit.SECONDS)
      .build(SpanBytesEncoder.JSON_V2);

    Thread.sleep(500); // wait for the thread to start

    reporter.report(span);

    long start = System.nanoTime();
    reporter.close(); // close while there's a pending span
    assertThat(System.nanoTime() - start)
      .isLessThan(TimeUnit.MILLISECONDS.toNanos(10)); // give wiggle room

    reporter.close();
  }

  @Test void flush_incrementsMetricsAndThrowsWhenClosed() {
    AsyncReporter<Span> reporter = AsyncReporter.newBuilder(sleepingSender)
      .metrics(metrics)
      .messageTimeout(0, TimeUnit.MILLISECONDS)
      .build(SpanBytesEncoder.JSON_V2);

    reporter.report(span);

    reporter.close();
    try {
      reporter.flush();
      failBecauseExceptionWasNotThrown(IllegalStateException.class);
    } catch (IllegalStateException e) {
      assertThat(metrics.spansDropped()).isEqualTo(1);
    }
  }

  @Test void flush_incrementsMetricsAndThrowsWhenSenderClosed() {
    AsyncReporter<Span> reporter = AsyncReporter.newBuilder(sleepingSender)
      .metrics(metrics)
      .messageTimeout(0, TimeUnit.MILLISECONDS)
      .build(SpanBytesEncoder.JSON_V2);

    reporter.report(span);

    sleepingSender.close();
    try {
      reporter.flush();
      failBecauseExceptionWasNotThrown(IllegalStateException.class);
    } catch (IllegalStateException e) {
      assertThat(metrics.spansDropped()).isEqualTo(1);
      assertThat(metrics.messagesDropped()).isEqualTo(1);
    } finally {
      reporter.close();
    }
  }

  @Test void build_threadFactory() {
    Thread thread = new Thread();
    AsyncReporter<Span> reporter = AsyncReporter.newBuilder(FakeSender.create())
      .threadFactory(r -> thread)
      .build(SpanBytesEncoder.JSON_V2);

    // Reporter thread is lazy
    assertThat(thread.isAlive()).isFalse();
    reporter.report(span);

    assertThat(thread.getName()).isEqualTo("AsyncReporter{FakeSender}");
    assertThat(thread.toString()).contains("AsyncReporter{FakeSender}");
    assertThat(thread.isDaemon()).isTrue();

    reporter.close();
    thread.interrupt();
  }

  @Test void build_proto3() {
    AsyncReporter.newBuilder(FakeSender.create().encoding(Encoding.PROTO3))
      .messageTimeout(0, TimeUnit.MILLISECONDS)
      .build(SpanBytesEncoder.PROTO3);
  }

  @Test void build_proto3_withCustomBytesEncoder() {
    AsyncReporter.newBuilder(FakeSender.create().encoding(Encoding.PROTO3))
      .messageTimeout(0, TimeUnit.MILLISECONDS)
      .build(new BytesEncoder<Span>() {
        @Override public Encoding encoding() {
          return Encoding.PROTO3;
        }

        @Override public int sizeInBytes(Span input) {
          return 0;
        }

        @Override public byte[] encode(Span input) {
          return new byte[0];
        }
      });
  }

  @Test void build_thrift() {
    AsyncReporter.newBuilder(FakeSender.create().encoding(Encoding.THRIFT))
      .messageTimeout(0, TimeUnit.MILLISECONDS)
      .build(SpanBytesEncoder.THRIFT);
  }

  @Test void build_thrift_withCustomBytesEncoder() {
    AsyncReporter.newBuilder(FakeSender.create().encoding(Encoding.THRIFT))
      .messageTimeout(0, TimeUnit.MILLISECONDS)
      .build(new BytesEncoder<Span>() {
        @Override public Encoding encoding() {
          return Encoding.THRIFT;
        }

        @Override public int sizeInBytes(Span input) {
          return 0;
        }

        @Override public byte[] encode(Span input) {
          return new byte[0];
        }
      });
  }
}
