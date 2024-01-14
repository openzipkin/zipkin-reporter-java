/*
 * Copyright 2016-2024 The OpenZipkin Authors
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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
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

  AsyncReporter<Span> reporter;
  InMemoryReporterMetrics metrics = new InMemoryReporterMetrics();

  @AfterEach void close() {
    if (reporter != null) reporter.close();
  }

  @Test void messageMaxBytes_defaultsToSender() {
    AtomicInteger sentSpans = new AtomicInteger();
    reporter = AsyncReporter.newBuilder(FakeSender.create()
        .onSpans(spans -> sentSpans.addAndGet(spans.size()))
        .messageMaxBytes(sizeInBytesOfSingleSpanMessage))
      .messageTimeout(0, TimeUnit.MILLISECONDS)
      .build(SpanBytesEncoder.JSON_V2);

    reporter.report(span);
    reporter.report(span); // drops
    reporter.flush();

    assertThat(sentSpans.get()).isEqualTo(1);
  }

  @Test void messageMaxBytes_dropsWhenOverqueuing() {
    AtomicInteger sentSpans = new AtomicInteger();
    reporter = AsyncReporter.newBuilder(FakeSender.create()
        .onSpans(spans -> sentSpans.addAndGet(spans.size())))
      .messageMaxBytes(sizeInBytesOfSingleSpanMessage)
      .messageTimeout(0, TimeUnit.MILLISECONDS)
      .build(SpanBytesEncoder.JSON_V2);

    reporter.report(span);
    reporter.report(span); // dropped the one that queued more than allowed bytes
    reporter.flush();

    assertThat(sentSpans.get()).isEqualTo(1);
  }

  @Test void messageMaxBytes_dropsWhenTooLarge() {
    AtomicInteger sentSpans = new AtomicInteger();
    reporter = AsyncReporter.newBuilder(FakeSender.create()
        .onSpans(spans -> sentSpans.addAndGet(spans.size())))
      .messageMaxBytes(sizeInBytesOfSingleSpanMessage)
      .messageTimeout(0, TimeUnit.MILLISECONDS)
      .build(SpanBytesEncoder.JSON_V2);

    reporter.report(span.toBuilder().addAnnotation(1L, "fooooo").build());
    reporter.flush();

    assertThat(sentSpans.get()).isEqualTo(0);
  }

  @Test void queuedMaxSpans_dropsWhenOverqueuing() {
    AtomicInteger sentSpans = new AtomicInteger();
    reporter = AsyncReporter.newBuilder(FakeSender.create()
        .onSpans(spans -> sentSpans.addAndGet(spans.size())))
      .queuedMaxSpans(1)
      .messageTimeout(0, TimeUnit.MILLISECONDS)
      .build(SpanBytesEncoder.JSON_V2);

    reporter.report(span);
    reporter.report(span); // dropped the one that queued more than allowed count
    reporter.flush();

    assertThat(sentSpans.get()).isEqualTo(1);
  }

  @Test void report_incrementsMetrics() {
    reporter = AsyncReporter.newBuilder(FakeSender.create())
      .metrics(metrics)
      .messageTimeout(0, TimeUnit.MILLISECONDS)
      .build(SpanBytesEncoder.JSON_V2);

    reporter.report(span);
    reporter.report(span);
    assertThat(metrics.spans()).isEqualTo(2);
    assertThat(metrics.spanBytes()).isEqualTo(SpanBytesEncoder.JSON_V2.encode(span).length * 2);
  }

  @Test void report_incrementsSpansDropped() {
    reporter = AsyncReporter.newBuilder(FakeSender.create())
      .queuedMaxSpans(1)
      .metrics(metrics)
      .messageTimeout(0, TimeUnit.MILLISECONDS)
      .build(SpanBytesEncoder.JSON_V2);

    reporter.report(span);
    reporter.report(span);

    assertThat(metrics.spans()).isEqualTo(2);
    assertThat(metrics.spansDropped()).isEqualTo(1);
  }

  @Test void flush_incrementsMetrics() {
    reporter = AsyncReporter.newBuilder(FakeSender.create())
      .metrics(metrics)
      .messageMaxBytes(sizeInBytesOfSingleSpanMessage)
      .messageTimeout(0, TimeUnit.MILLISECONDS)
      .build(SpanBytesEncoder.JSON_V2);

    // Queue up 2 spans
    reporter.report(span);
    reporter.report(span);

    reporter.flush();
    assertThat(metrics.queuedSpans()).isEqualTo(1); // still one span in the backlog
    assertThat(metrics.queuedBytes()).isEqualTo(SpanBytesEncoder.JSON_V2.encode(span).length);
    assertThat(metrics.messages()).isEqualTo(1);
    assertThat(metrics.messageBytes()).isEqualTo(sizeInBytesOfSingleSpanMessage);

    reporter.flush();
    assertThat(metrics.queuedSpans()).isZero();
    assertThat(metrics.queuedBytes()).isZero();
    assertThat(metrics.messages()).isEqualTo(2);
    assertThat(metrics.messageBytes()).isEqualTo(sizeInBytesOfSingleSpanMessage * 2);
  }

  @Test void flush_incrementsMessagesDropped() {
    reporter = AsyncReporter.newBuilder(FakeSender.create()
        .onSpans(spans -> {
          throw new RuntimeException();
        }))
      .metrics(metrics)
      .messageTimeout(0, TimeUnit.MILLISECONDS)
      .build(SpanBytesEncoder.JSON_V2);

    reporter.report(span);

    reporter.flush();
    assertThat(metrics.messagesDropped()).isEqualTo(1);
  }

  @Test void flush_logsFirstErrorAsWarn() {
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

    reporter = AsyncReporter.newBuilder(FakeSender.create()
        .onSpans(spans -> {
          throw new RuntimeException();
        }))
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
  }

  /** It can take up to the messageTimeout past the first span to send */
  @Test void messageTimeout_flushesWhenTimeoutExceeded() throws InterruptedException {
    CountDownLatch sentSpans = new CountDownLatch(1);
    reporter = AsyncReporter.newBuilder(FakeSender.create()
        .onSpans(spans -> sentSpans.countDown()))
      .messageTimeout(10, TimeUnit.MILLISECONDS)
      .build(SpanBytesEncoder.JSON_V2);

    reporter.report(span);
    assertThat(sentSpans.await(5, TimeUnit.MILLISECONDS))
      .isFalse();
    assertThat(sentSpans.await(10, TimeUnit.MILLISECONDS))
      .isTrue();
  }

  @Test void messageTimeout_disabled() throws InterruptedException {
    CountDownLatch sentSpans = new CountDownLatch(1);
    reporter = AsyncReporter.newBuilder(FakeSender.create()
        .onSpans(spans -> sentSpans.countDown()))
      .messageTimeout(0, TimeUnit.NANOSECONDS)
      .build(SpanBytesEncoder.JSON_V2);

    reporter.report(span);
    assertThat(sentSpans.getCount()).isEqualTo(1);

    // Since no threads started, the above lingers
    assertThat(sentSpans.await(10, TimeUnit.MILLISECONDS))
      .isFalse();
  }

  @Test void senderThread_threadHasAPrettyName() throws InterruptedException {
    BlockingQueue<String> threadName = new LinkedBlockingQueue<>();
    reporter = AsyncReporter.newBuilder(FakeSender.create()
        .onSpans(spans -> threadName.offer(Thread.currentThread().getName())))
      .build(SpanBytesEncoder.JSON_V2);

    reporter.report(span);

    // check name is pretty
    assertThat(threadName.take())
      .isEqualTo("AsyncReporter{FakeSender}");
  }

  @Test void close_close_stopsFlushThread() throws InterruptedException {
    reporter = AsyncReporter.newBuilder(FakeSender.create())
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
  }

  @Test void flush_throwsOnClose() {
    assertThrows(IllegalStateException.class, () -> {
      reporter = AsyncReporter.newBuilder(FakeSender.create())
        .metrics(metrics)
        .messageTimeout(0, TimeUnit.MILLISECONDS)
        .build(SpanBytesEncoder.JSON_V2);

      reporter.report(span);
      reporter.close(); // close while there's a pending span
      reporter.flush();
    });
  }

  @Test void report_doesntThrowWhenClosed() {
    reporter = AsyncReporter.newBuilder(FakeSender.create())
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
    reporter = AsyncReporter.newBuilder(sleepingSender)
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
  }

  @Test void senderThread_dropsOnReporterClose_flushThread() throws InterruptedException {
    CountDownLatch received = new CountDownLatch(1);
    CountDownLatch sent = new CountDownLatch(1);
    reporter = AsyncReporter.newBuilder(FakeSender.create()
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

  @Test void blocksToClearPendingSpans() throws InterruptedException {
    reporter = AsyncReporter.newBuilder(FakeSender.create())
      .metrics(metrics)
      .messageTimeout(30, TimeUnit.SECONDS)
      .build(SpanBytesEncoder.JSON_V2);

    reporter.report(span);
    Thread.sleep(500); // wait for the thread to start

    reporter.close(); // close while there's a pending span

    assertThat(metrics.spans()).isEqualTo(1);
    assertThat(metrics.spansDropped()).isEqualTo(0);
  }

  @Test void quitsBlockingWhenOverTimeout() throws InterruptedException {
    reporter = AsyncReporter.newBuilder(FakeSender.create()
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
      .closeTimeout(1, TimeUnit.NANOSECONDS)
      .messageTimeout(30, TimeUnit.SECONDS)
      .build(SpanBytesEncoder.JSON_V2);

    Thread.sleep(500); // wait for the thread to start

    reporter.report(span);

    long start = System.nanoTime();
    reporter.close(); // close while there's a pending span
    assertThat(System.nanoTime() - start)
      .isLessThan(TimeUnit.MILLISECONDS.toNanos(10)); // give wiggle room
  }

  @Test void flush_incrementsMetricsAndThrowsWhenClosed() {
    reporter = AsyncReporter.newBuilder(sleepingSender)
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
    reporter = AsyncReporter.newBuilder(sleepingSender)
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
    }
  }

  @Test void build_threadFactory() {
    Thread thread = new Thread();
    reporter = AsyncReporter.newBuilder(FakeSender.create())
      .threadFactory(r -> thread)
      .build(SpanBytesEncoder.JSON_V2);

    // Reporter thread is lazy
    assertThat(thread.isAlive()).isFalse();
    reporter.report(span);

    assertThat(thread.getName()).isEqualTo("AsyncReporter{FakeSender}");
    assertThat(thread.toString()).contains("AsyncReporter{FakeSender}");
    assertThat(thread.isDaemon()).isTrue();

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
