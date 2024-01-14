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
package zipkin2.reporter;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import zipkin2.Span;
import zipkin2.TestObjects;
import zipkin2.codec.SpanBytesDecoder;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Only tests entry points as {@link zipkin2.reporter.internal.AsyncReporter} tests covers the rest.
 */
class AsyncReporterTest {
  @Test void example() {
    AtomicInteger sentSpans = new AtomicInteger();
    try (AsyncReporter<Span> reporter = AsyncReporter.builder(FakeSender.create()
        .onSpans(spans -> sentSpans.addAndGet(spans.size())))
      .messageTimeout(0, TimeUnit.MILLISECONDS) // no thread
      .build(SpanBytesEncoder.JSON_V2)) {

      reporter.report(TestObjects.CLIENT_SPAN);
      reporter.flush();
    }

    assertThat(sentSpans.get()).isEqualTo(1);
  }

  @Deprecated @Test void example_deprecatedSender() {
    AtomicInteger sentSpans = new AtomicInteger();
    try (AsyncReporter<Span> reporter = AsyncReporter.builder(new DeprecatedCheatingSender(
        spans -> sentSpans.addAndGet(spans.size())
      ))
      .messageTimeout(0, TimeUnit.MILLISECONDS) // no thread
      .build(SpanBytesEncoder.JSON_V2)) {

      reporter.report(TestObjects.CLIENT_SPAN);
      reporter.flush();
    }

    assertThat(sentSpans.get()).isEqualTo(1);
  }

  @Deprecated static class DeprecatedCheatingSender extends Sender {
    final Consumer<List<Span>> onSpans;

    DeprecatedCheatingSender(Consumer<List<Span>> onSpans) {
      this.onSpans = onSpans;
    }

    @Override public Encoding encoding() {
      return Encoding.JSON;
    }

    @Override public int messageMaxBytes() {
      return 500;
    }

    @Override public int messageSizeInBytes(List<byte[]> encodedSpans) {
      return Encoding.JSON.listSizeInBytes(encodedSpans);
    }

    @Override public int messageSizeInBytes(int encodedSizeInBytes) {
      return Encoding.JSON.listSizeInBytes(encodedSizeInBytes);
    }

    @Override public Call<Void> sendSpans(List<byte[]> encodedSpans) {
      List<Span> decoded = encodedSpans.stream()
        .map(SpanBytesDecoder.JSON_V2::decodeOne).
        collect(Collectors.toList());
      return new CheatingVoidCall(onSpans, decoded);
    }
  }

  @Deprecated static class CheatingVoidCall extends Call<Void> {
    final Consumer<List<Span>> onSpans;
    final List<Span> spans;

    CheatingVoidCall(Consumer<List<Span>> onSpans, List<Span> spans) {
      this.onSpans = onSpans;
      this.spans = spans;
    }

    @Override public Void execute() {
      onSpans.accept(spans);
      return null;
    }

    @Override public void enqueue(Callback<Void> callback) {
      throw new UnsupportedOperationException();
    }

    @Override public void cancel() {
      throw new UnsupportedOperationException();
    }

    @Override public boolean isCanceled() {
      return false;
    }

    @Override public Call<Void> clone() {
      throw new UnsupportedOperationException();
    }
  }
}
