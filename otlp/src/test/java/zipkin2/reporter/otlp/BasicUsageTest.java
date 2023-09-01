/*
 * Copyright 2016-2023 The OpenZipkin Authors
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
package zipkin2.reporter.otlp;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import brave.Span;
import brave.Span.Kind;
import brave.Tracer;
import brave.Tracing;
import brave.handler.SpanHandler;
import brave.propagation.ThreadLocalCurrentTraceContext;
import brave.sampler.Sampler;
import okhttp3.OkHttpClient;
import okhttp3.OkHttpClient.Builder;
import okhttp3.Request;
import okhttp3.Response;
import org.assertj.core.api.BDDAssertions;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import zipkin2.codec.Encoding;
import zipkin2.reporter.okhttp3.OkHttpSender;

@Testcontainers
class BasicUsageTest {

  @Container
  static JaegerAllInOne jaegerAllInOne = new JaegerAllInOne();

  TestSetup testSetup;

  Tracing tracing;

  @ParameterizedTest
  @EnumSource(TestSetup.class)
  void shouldSendSpansToOtlpEndpoint(TestSetup testSetup) throws InterruptedException, IOException {
    // Setup
    ThreadLocalCurrentTraceContext braveCurrentTraceContext = ThreadLocalCurrentTraceContext.newBuilder()
      .build();
    this.testSetup = testSetup;
    SpanHandler spanHandler = testSetup.apply(jaegerAllInOne.getHttpOtlpPort());
    tracing = Tracing.newBuilder()
      .currentTraceContext(braveCurrentTraceContext)
      .supportsJoin(false)
      .traceId128Bit(true)
      .sampler(Sampler.ALWAYS_SAMPLE)
      .addSpanHandler(spanHandler)
      .localServiceName("my-service")
      .build();
    Tracer braveTracer = tracing.tracer();

    List<String> traceIds = new ArrayList<>();
    final int size = 5;
    for (int i = 0; i < size; i++) {
      // Given
      Span span = braveTracer.nextSpan().name("foo " + i)
        .tag("foo tag", "foo value")
        .kind(Kind.CONSUMER)
        .error(new RuntimeException("BOOOOOM!"))
        .remoteServiceName("remote service")
        .start();
      String traceId = span.context().traceIdString();
      System.out.println("Trace Id <" + traceId + ">");
      span.remoteIpAndPort("http://localhost", 123456);
      Thread.sleep(50);
      span.annotate("boom!");
      Thread.sleep(50);

      // When
      span.finish();
      traceIds.add(span.context().traceIdString());
    }

    testSetup.close();

    // Then
    Awaitility.await().untilAsserted(() -> {
      BDDAssertions.then(traceIds).hasSize(size);
      OkHttpClient client = new Builder()
        .build();
      traceIds.forEach(traceId -> {
        Request request = new Request.Builder().url("http://localhost:" + jaegerAllInOne.getQueryPort() + "/api/traces/" + traceId).build();
        try (Response response = client.newCall(request).execute()) {
          BDDAssertions.then(response.isSuccessful()).isTrue();
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
      });

    });
  }

  enum TestSetup implements Function<Integer, SpanHandler>, Closeable {

    SYNC_HANDLER_OK_HTTP {

      OkHttpSender okHttpSender;

      SyncOtlpReporter reporter;

      OtlpSpanHandler spanHandler;

      @Override
      public void close() {
        if (reporter != null) {
          reporter.close();
        }
        if (okHttpSender != null) {
          okHttpSender.close();
        }
        if (spanHandler != null) {
          spanHandler.close();
        }
      }

      @Override
      public SpanHandler apply(Integer port) {
        okHttpSender = OkHttpSender.newBuilder()
          .encoding(Encoding.PROTO3)
          .endpoint("http://localhost:" + port + "/v1/traces")
          .build();
        reporter = (SyncOtlpReporter) SyncOtlpReporter.create(okHttpSender);
        spanHandler = (OtlpSpanHandler) OtlpSpanHandler.create(reporter);
        return spanHandler;
      }
    },

    ASYNC_HANDLER_OK_HTTP_CUSTOM_REPORTER {

      OkHttpSender okHttpSender;

      SyncOtlpReporter reporter;

      SpanHandler spanHandler;

      @Override
      public void close() {
        if (reporter != null) {
          reporter.close();
        }
        if (okHttpSender != null) {
          okHttpSender.close();
        }
      }

      @Override
      public SpanHandler apply(Integer port) {
        okHttpSender = OkHttpSender.newBuilder()
          .encoding(Encoding.PROTO3)
          .endpoint("http://localhost:" + port + "/v1/traces")
          .build();
        reporter = (SyncOtlpReporter) SyncOtlpReporter.create(okHttpSender);
        spanHandler = AsyncOtlpSpanHandler.newBuilder(reporter)
          .build();
        return spanHandler;
      }
    },

    ASYNC_OK_HTTP_DEFAULT_REPORTER {

      OkHttpSender okHttpSender;

      AsyncOtlpSpanHandler spanHandler;

      @Override
      public void close() {
        if (spanHandler != null) {
          spanHandler.flush();
          spanHandler.close();
        }
        if (okHttpSender != null) {
          okHttpSender.close();
        }
      }

      @Override
      public SpanHandler apply(Integer port) {
        okHttpSender = OkHttpSender.newBuilder()
          .encoding(Encoding.PROTO3)
          .endpoint("http://localhost:" + port + "/v1/traces")
          .build();
        spanHandler = AsyncOtlpSpanHandler.create(okHttpSender);
        return spanHandler;
      }

    }
  }

  @AfterEach
  void shutdown() throws IOException {
    if (tracing != null) {
      tracing.close();
    }
  }
}
