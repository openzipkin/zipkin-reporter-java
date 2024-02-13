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
package zipkin2.reporter.brave;

import brave.handler.MutableSpan;
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
import zipkin2.reporter.BytesEncoder;

import static zipkin2.reporter.brave.MutableSpans.newBigClientSpan;
import static zipkin2.reporter.brave.MutableSpans.newServerSpan;

@Measurement(iterations = 5, time = 1)
@Warmup(iterations = 10, time = 1)
@Fork(3)
@BenchmarkMode(Mode.SampleTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Thread)
@Threads(1)
public class MutableSpanBytesEncoderBenchmarks {

  static final BytesEncoder<MutableSpan> jsonEncoder = MutableSpanBytesEncoder.JSON_V2;
  static final BytesEncoder<MutableSpan> protoEncoder = MutableSpanBytesEncoder.PROTO3;
  static final MutableSpan serverSpan = newServerSpan();
  static final MutableSpan bigClientSpan = newBigClientSpan();

  @Benchmark public int sizeInBytes_serverSpan_json() {
    return jsonEncoder.sizeInBytes(serverSpan);
  }

  @Benchmark public int sizeInBytes_serverSpan_proto() {
    return protoEncoder.sizeInBytes(serverSpan);
  }

  @Benchmark public byte[] encode_serverSpan_json() {
    return jsonEncoder.encode(serverSpan);
  }

  @Benchmark public byte[] encode_serverSpan_proto() {
    return protoEncoder.encode(serverSpan);
  }

  @Benchmark public int sizeInBytes_bigClientSpan_json() {
    return jsonEncoder.sizeInBytes(bigClientSpan);
  }

  @Benchmark public int sizeInBytes_bigClientSpan_proto() {
    return protoEncoder.sizeInBytes(bigClientSpan);
  }

  @Benchmark public byte[] encode_bigClientSpan_json() {
    return jsonEncoder.encode(bigClientSpan);
  }

  @Benchmark public byte[] encode_bigClientSpan_proto() {
    return protoEncoder.encode(bigClientSpan);
  }

  // Convenience main entry-point
  public static void main(String[] args) throws RunnerException {
    Options opt = new OptionsBuilder()
      .addProfiler("gc")
      .include(".*" + MutableSpanBytesEncoderBenchmarks.class.getSimpleName())
      .build();

    new Runner(opt).run();
  }
}
