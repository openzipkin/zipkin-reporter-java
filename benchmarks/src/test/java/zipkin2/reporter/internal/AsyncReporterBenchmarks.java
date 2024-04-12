/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin2.reporter.internal;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import org.openjdk.jmh.annotations.AuxCounters;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Group;
import org.openjdk.jmh.annotations.GroupThreads;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import zipkin2.Span;
import zipkin2.TestObjects;
import zipkin2.reporter.BytesEncoder;
import zipkin2.reporter.Encoding;
import zipkin2.reporter.InMemoryReporterMetrics;
import zipkin2.reporter.SpanBytesEncoder;

@Measurement(iterations = 5, time = 1)
@Warmup(iterations = 10, time = 1)
@Fork(3)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Group)
public class AsyncReporterBenchmarks {
  static final Span clientSpan = TestObjects.CLIENT_SPAN;
  static final InMemoryReporterMetrics metrics = new InMemoryReporterMetrics();
  static final AtomicLong spanBacklog = new AtomicLong();

  @Param
  public Encoding encoding;

  @Param({"0", "20000000"})
  public int maxBytes;

  @AuxCounters
  @State(Scope.Thread)
  public static class InMemoryReporterMetricsAsCounters {

    public long spans() {
      return metrics.spans();
    }

    public long spansDropped() {
      return metrics.spansDropped();
    }

    public long messages() {
      return metrics.messages();
    }

    public long messagesDropped() {
      return metrics.messagesDropped();
    }

    public long spanBacklog() {
      return spanBacklog.get();
    }

    @Setup(Level.Iteration)
    public void clean() {
      metrics.clear();
      spanBacklog.set(0);
    }
  }

  AsyncReporter<Span> reporter;

  @Setup(Level.Trial)
  public void setup() {
    final BytesEncoder<Span> encoder = Stream
      .of(SpanBytesEncoder.JSON_V2, SpanBytesEncoder.PROTO3, SpanBytesEncoder.THRIFT)
      .filter(e -> e.encoding().equals(encoding))
      .findAny()
      .orElseThrow(() -> new IllegalStateException("Unable to find BytesEncoder<Span> for " + encoding));

    reporter = AsyncReporter.newBuilder(new NoopSender(encoding))
      .messageMaxBytes(1000000) // example default from Kafka message.max.bytes
      .queuedMaxBytes(maxBytes)
      .metrics(metrics)
      .build(encoder);
  }

  @Benchmark @Group("no_contention") @GroupThreads(1)
  public void no_contention_report(InMemoryReporterMetricsAsCounters counters) {
    reporter.report(clientSpan);
  }

  @Benchmark @Group("mild_contention") @GroupThreads(2)
  public void mild_contention_report(InMemoryReporterMetricsAsCounters counters) {
    reporter.report(clientSpan);
  }

  @Benchmark @Group("high_contention") @GroupThreads(8)
  public void high_contention_report(InMemoryReporterMetricsAsCounters counters) {
    reporter.report(clientSpan);
  }

  @TearDown(Level.Iteration)
  public void clear() {
    spanBacklog.addAndGet(((AsyncReporter.BoundedAsyncReporter) reporter).pending.clear());
  }
}
