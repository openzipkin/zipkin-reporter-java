/**
 * Copyright 2016 The OpenZipkin Authors
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
package zipkin.reporter;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.After;
import org.junit.Test;
import zipkin.Annotation;
import zipkin.Codec;
import zipkin.Span;
import zipkin.TestObjects;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;

public class AsyncReporterTest {

  Span span = TestObjects.TRACE.get(2);
  int sizeInBytesOfSingleSpanMessage =
      MessageEncoder.THRIFT_BYTES.overheadInBytes(1) + Encoder.THRIFT_BYTES.encode(span).length;

  AsyncReporter<Span> reporter;
  InMemoryReporterMetrics metrics = new InMemoryReporterMetrics();

  @After
  public void close() {
    if (reporter != null) reporter.close();
  }

  @Test
  public void messageMaxBytes_defaultsToSender() {
    AtomicInteger sentSpans = new AtomicInteger();
    reporter = AsyncReporter.builder(FakeSender.create()
        .onSpans(spans -> sentSpans.addAndGet(spans.size()))
        .messageMaxBytes(sizeInBytesOfSingleSpanMessage))
        .messageTimeout(0, TimeUnit.MILLISECONDS)
        .build(Encoder.THRIFT_BYTES);

    reporter.report(span);
    reporter.report(span); // drops
    reporter.flush();

    assertThat(sentSpans.get()).isEqualTo(1);
  }

  @Test
  public void messageMaxBytes_dropsWhenOverqueuing() {
    AtomicInteger sentSpans = new AtomicInteger();
    reporter = AsyncReporter.builder(FakeSender.create()
        .onSpans(spans -> sentSpans.addAndGet(spans.size())))
        .messageMaxBytes(sizeInBytesOfSingleSpanMessage)
        .messageTimeout(0, TimeUnit.MILLISECONDS)
        .build(Encoder.THRIFT_BYTES);

    reporter.report(span);
    reporter.report(span); // dropped the one that queued more than allowed bytes
    reporter.flush();

    assertThat(sentSpans.get()).isEqualTo(1);
  }

  @Test
  public void messageMaxBytes_dropsWhenTooLarge() {
    AtomicInteger sentSpans = new AtomicInteger();
    reporter = AsyncReporter.builder(FakeSender.create()
        .onSpans(spans -> sentSpans.addAndGet(spans.size())))
        .messageMaxBytes(sizeInBytesOfSingleSpanMessage)
        .messageTimeout(0, TimeUnit.MILLISECONDS)
        .build(Encoder.THRIFT_BYTES);

    reporter.report(span.toBuilder().addAnnotation(Annotation.create(1L, "fooooo", null)).build());
    reporter.flush();

    assertThat(sentSpans.get()).isEqualTo(0);
  }

  @Test
  public void queuedMaxSpans_dropsWhenOverqueuing() {
    AtomicInteger sentSpans = new AtomicInteger();
    reporter = AsyncReporter.builder(FakeSender.create()
        .onSpans(spans -> sentSpans.addAndGet(spans.size())))
        .queuedMaxSpans(1)
        .messageTimeout(0, TimeUnit.MILLISECONDS)
        .build(Encoder.THRIFT_BYTES);

    reporter.report(span);
    reporter.report(span); // dropped the one that queued more than allowed count
    reporter.flush();

    assertThat(sentSpans.get()).isEqualTo(1);
  }

  @Test
  public void report_incrementsMetrics() {
    reporter = AsyncReporter.builder(FakeSender.create())
        .metrics(metrics)
        .messageTimeout(0, TimeUnit.MILLISECONDS)
        .build(Encoder.THRIFT_BYTES);

    reporter.report(span);
    reporter.report(span);
    assertThat(metrics.spans()).isEqualTo(2);
    assertThat(metrics.spanBytes()).isEqualTo(Encoder.THRIFT_BYTES.encode(span).length * 2);
  }

  @Test
  public void report_incrementsSpansDropped() {
    reporter = AsyncReporter.builder(FakeSender.create())
        .queuedMaxSpans(1)
        .metrics(metrics)
        .messageTimeout(0, TimeUnit.MILLISECONDS)
        .build(Encoder.THRIFT_BYTES);

    reporter.report(span);
    reporter.report(span);

    assertThat(metrics.spans()).isEqualTo(2);
    assertThat(metrics.spansDropped()).isEqualTo(1);
  }

  @Test
  public void flush_incrementsMetrics() {
    reporter = AsyncReporter.builder(FakeSender.create())
        .metrics(metrics)
        .messageTimeout(0, TimeUnit.MILLISECONDS)
        .build(Encoder.THRIFT_BYTES);

    reporter.report(span);
    reporter.report(span);

    reporter.flush();
    assertThat(metrics.messages()).isEqualTo(1);
    assertThat(metrics.messageBytes()).isEqualTo(
        Codec.THRIFT.writeSpans(asList(span, span)).length);
  }

  @Test
  public void flush_incrementsMessagesDropped() {
    reporter = AsyncReporter.builder(FakeSender.create()
        .onSpans(spans -> {
          throw new RuntimeException();
        }))
        .metrics(metrics)
        .messageTimeout(0, TimeUnit.MILLISECONDS)
        .build(Encoder.THRIFT_BYTES);

    reporter.report(span);

    reporter.flush();
    assertThat(metrics.messagesDropped()).isEqualTo(1);
  }

  /** It can take up to the messageTimeout past the first span to send */
  @Test
  public void messageTimeout_flushesWhenTimeoutExceeded() throws InterruptedException {
    CountDownLatch sentSpans = new CountDownLatch(1);
    reporter = AsyncReporter.builder(FakeSender.create()
        .onSpans(spans -> sentSpans.countDown()))
        .messageTimeout(10, TimeUnit.MILLISECONDS)
        .build(Encoder.THRIFT_BYTES);

    reporter.report(span);
    assertThat(sentSpans.await(5, TimeUnit.MILLISECONDS))
        .isFalse();
    assertThat(sentSpans.await(10, TimeUnit.MILLISECONDS))
        .isTrue();
  }

  @Test
  public void messageTimeout_disabled() throws InterruptedException {
    CountDownLatch sentSpans = new CountDownLatch(1);
    reporter = AsyncReporter.builder(FakeSender.create()
        .onSpans(spans -> sentSpans.countDown()))
        .messageTimeout(0, TimeUnit.NANOSECONDS)
        .build(Encoder.THRIFT_BYTES);

    reporter.report(span);
    assertThat(sentSpans.getCount()).isEqualTo(1);

    // Since no threads started, the above lingers
    assertThat(sentSpans.await(10, TimeUnit.MILLISECONDS))
        .isFalse();
  }

  @Test
  public void senderThread_threadHasAPrettyName() throws InterruptedException {
    BlockingQueue<String> threadName = new LinkedBlockingQueue<>();
    reporter = AsyncReporter.builder(FakeSender.create()
        .onSpans(spans -> threadName.offer(Thread.currentThread().getName())))
        .build(Encoder.THRIFT_BYTES);

    reporter.report(span);

    // check name is pretty
    assertThat(threadName.take())
        .isEqualTo("AsyncReporter(FakeSender)");
  }

  @Test
  public void close_stopsSender() throws InterruptedException {
    AtomicInteger sendCount = new AtomicInteger();
    reporter = AsyncReporter.builder(FakeSender.create()
        .onSpans(spans -> {
          sendCount.incrementAndGet();
          try {
            Thread.sleep(10); // this blocks the loop
          } catch (InterruptedException e) {
          }
        }))
        .metrics(metrics)
        .messageTimeout(2, TimeUnit.MILLISECONDS)
        .build(Encoder.THRIFT_BYTES);

    reporter.report(span);
    Thread.sleep(5);
    reporter.report(span); // report while sender is blocked
    reporter.close(); // close before sender can send the next message
    Thread.sleep(5);

    assertThat(sendCount.get()).isEqualTo(1);
    assertThat(metrics.spansDropped()).isEqualTo(1);
  }

  @Test
  public void senderThread_pushesBackOnClose() throws InterruptedException {
    AtomicInteger sendCount = new AtomicInteger();
    reporter = AsyncReporter.builder(FakeSender.create())
        .metrics(metrics)
        .messageTimeout(10, TimeUnit.MILLISECONDS)
        .build(Encoder.THRIFT_BYTES);

    reporter.report(span);
    Thread.sleep(5); // flush thread got the first span, but still waiting for more
    reporter.close(); // close while there's a pending span
    Thread.sleep(10); // wait for the poll to unblock

    assertThat(sendCount.get()).isZero();
    assertThat(metrics.spansDropped()).isEqualTo(1);
  }

  @Test(expected = IllegalStateException.class)
  public void flush_throwsOnClose() {
    reporter = AsyncReporter.builder(FakeSender.create())
        .metrics(metrics)
        .messageTimeout(0, TimeUnit.MILLISECONDS)
        .build(Encoder.THRIFT_BYTES);

    reporter.report(span);
    reporter.close(); // close while there's a pending span
    reporter.flush();
  }

  @Test
  public void report_doesntThrowWhenClosed() {
    reporter = AsyncReporter.builder(FakeSender.create())
        .metrics(metrics)
        .messageTimeout(0, TimeUnit.MILLISECONDS)
        .build(Encoder.THRIFT_BYTES);

    reporter.close();

    // Don't throw, as that could crash apps. Just increment drop metrics
    reporter.report(span);
    assertThat(metrics.spansDropped()).isEqualTo(1);
  }
}
