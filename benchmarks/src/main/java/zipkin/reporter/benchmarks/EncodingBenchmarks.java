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
package zipkin.reporter.benchmarks;

import java.util.ArrayList;
import java.util.List;
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
import zipkin.Span;
import zipkin.TestObjects;
import zipkin.reporter.Encoder;
import zipkin.reporter.MessageEncoder;

@Measurement(iterations = 5, time = 1)
@Warmup(iterations = 10, time = 1)
@Fork(3)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Thread)
@Threads(1)
public class EncodingBenchmarks {
  static final Span clientSpan = TestObjects.TRACE.get(2);
  static final List<byte[]> clientSpansJson = encode100Spans(Encoder.JSON);
  static final List<byte[]> clientSpansThrift = encode100Spans(Encoder.THRIFT);

  @Benchmark
  public List<byte[]> encode100Spans_thrift() {
    return encode100Spans(Encoder.THRIFT);
  }

  @Benchmark
  public List<byte[]> encode100Spans_json() {
    return encode100Spans(Encoder.JSON);
  }

  @Benchmark
  public byte[] encodeListOf100Spans_thrift() {
    return MessageEncoder.THRIFT_BYTES.encode(clientSpansThrift);
  }

  @Benchmark
  public byte[] encodeListOf100Spans_json() {
    return MessageEncoder.JSON_BYTES.encode(clientSpansJson);
  }

  static List<byte[]> encode100Spans(Encoder<Span> encoder) {
    List<byte[]> spans = new ArrayList<>(100);
    for (int i = 0; i < 100; i++) {
      spans.add(encoder.encode(clientSpan));
    }
    return spans;
  }

  // Convenience main entry-point
  public static void main(String[] args) throws RunnerException {
    Options opt = new OptionsBuilder()
        .include(".*" + EncodingBenchmarks.class.getSimpleName() + ".*")
        .build();

    new Runner(opt).run();
  }
}
