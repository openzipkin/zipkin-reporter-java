/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package zipkin2.reporter;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
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
import zipkin2.codec.Encoding;

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
    reporter = AsyncReporter.builder(new NoopSender(encoding))
        .messageMaxBytes(1000000) // example default from Kafka message.max.bytes
        .metrics(metrics)
        .build();
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
  public void clear() throws IOException {
    spanBacklog.addAndGet(((AsyncReporter.BoundedAsyncReporter) reporter).pending.clear());
  }
}
