/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin2.reporter.brave;

import brave.handler.MutableSpan;
import brave.handler.SpanHandler;
import brave.propagation.TraceContext;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import zipkin2.reporter.Reporter;

import static zipkin2.reporter.brave.MutableSpans.newBigClientSpan;
import static zipkin2.reporter.brave.MutableSpans.newServerSpan;

@Measurement(iterations = 5, time = 1)
@Warmup(iterations = 10, time = 1)
@Fork(3)
@BenchmarkMode(Mode.SampleTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Thread)
@Threads(1)
public class ZipkinSpanHandlerBenchmarks {
  final SpanHandler handler =
    ZipkinSpanHandler.newBuilder(Reporter.NOOP).alwaysReportSpans(true).build();
  final TraceContext context = TraceContext.newBuilder().traceId(1).spanId(2).sampled(true).build();
  final MutableSpan serverSpan = newServerSpan();
  final MutableSpan bigClientSpan = newBigClientSpan();

  @Benchmark public boolean handleServerSpan() {
    return handler.end(context, serverSpan, SpanHandler.Cause.FINISHED);
  }

  @Benchmark public boolean handleBigClientSpan() {
    return handler.end(context, bigClientSpan, SpanHandler.Cause.FINISHED);
  }

  // Convenience main entry-point
  public static void main(String[] args) throws RunnerException {
    Options opt = new OptionsBuilder()
      .addProfiler("gc")
      .include(".*" + ZipkinSpanHandlerBenchmarks.class.getSimpleName() + ".*")
      .build();

    new Runner(opt).run();
  }
}
