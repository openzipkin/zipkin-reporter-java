/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin2.reporter.internal;

import java.util.concurrent.TimeUnit;
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
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import zipkin2.reporter.ReporterMetrics;

@Measurement(iterations = 5, time = 1)
@Warmup(iterations = 10, time = 1)
@Fork(3)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Group)
public class ByteBoundedQueueBenchmarks {
  static final byte ONE = 1;

  @AuxCounters
  @State(Scope.Thread)
  public static class OfferCounters {
    public int offersFailed;
    public int offersMade;

    @Setup(Level.Iteration)
    public void clean() {
      offersFailed = offersMade = 0;
    }
  }

  @AuxCounters
  @State(Scope.Thread)
  public static class DrainCounters {
    public int drained;

    @Setup(Level.Iteration)
    public void clean() {
      drained = 0;
    }
  }

  private static ThreadLocal<Object> marker = new ThreadLocal<>();

  @State(Scope.Thread)
  public static class ConsumerMarker {
    public ConsumerMarker() {
      marker.set(this);
    }
  }

  ByteBoundedQueue<Byte> q;

  @Setup
  public void setup() {
    q = new ByteBoundedQueue<>(ReporterMetrics.NOOP_METRICS, 10000, 10000);
  }

  @Benchmark @Group("no_contention") @GroupThreads(1)
  public void no_contention_offer(OfferCounters counters) {
    if (q.offer(ONE, 1)) {
      counters.offersMade++;
    } else {
      counters.offersFailed++;
    }
  }

  @Benchmark @Group("no_contention") @GroupThreads(1)
  public void no_contention_drain(DrainCounters counters, ConsumerMarker cm) {
    q.drainTo((s, b) -> {
      counters.drained++;
      return true;
    }, 1000);
  }

  @Benchmark @Group("mild_contention") @GroupThreads(2)
  public void mild_contention_offer(OfferCounters counters) {
    if (q.offer(ONE, 1)) {
      counters.offersMade++;
    } else {
      counters.offersFailed++;
    }
  }

  @Benchmark @Group("mild_contention") @GroupThreads(1)
  public void mild_contention_drain(DrainCounters counters, ConsumerMarker cm) {
    q.drainTo((s, b) -> {
      counters.drained++;
      return true;
    }, 1000);
  }

  @Benchmark @Group("high_contention") @GroupThreads(8)
  public void high_contention_offer(OfferCounters counters) {
    if (q.offer(ONE, 1)) {
      counters.offersMade++;
    } else {
      counters.offersFailed++;
    }
  }

  @Benchmark @Group("high_contention") @GroupThreads(1)
  public void high_contention_drain(DrainCounters counters, ConsumerMarker cm) {
    q.drainTo((s, b) -> {
      counters.drained++;
      return true;
    }, 1000);
  }

  @TearDown(Level.Iteration)
  public void emptyQ() {
    // If this thread didn't drain, return
    if (marker.get() == null) return;
    q.clear();
  }

  // Convenience main entry-point
  public static void main(String[] args) throws RunnerException {
    Options opt = new OptionsBuilder()
      .include(".*" + ByteBoundedQueueBenchmarks.class.getSimpleName() + ".*")
      .build();

    new Runner(opt).run();
  }
}
