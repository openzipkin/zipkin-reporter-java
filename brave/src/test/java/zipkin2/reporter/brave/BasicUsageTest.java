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

import brave.Tracing;
import brave.handler.SpanHandler;
import brave.propagation.B3SingleFormat;
import brave.propagation.TraceContext;
import brave.propagation.TraceContextOrSamplingFlags;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import zipkin2.Span;

import static org.assertj.core.api.Assertions.assertThat;

abstract class BasicUsageTest<H extends SpanHandler> {
  List<Span> spans = new ArrayList<>();
  H zipkinSpanHandler;
  Tracing tracing;

  abstract H zipkinSpanHandler(List<Span> spans);

  abstract void close(H handler);

  abstract SpanHandler alwaysReportSpans(H handler);

  @BeforeEach void init() {
    zipkinSpanHandler = zipkinSpanHandler(spans);
    tracing = Tracing.newBuilder()
      .localServiceName("Aa")
      .localIp("1.2.3.4")
      .localPort(80)
      .addSpanHandler(zipkinSpanHandler)
      .build();
  }

  @AfterEach void close() {
    tracing.close();
  }

  @Test void reconfigure() {
    tracing.close();
    close(zipkinSpanHandler);

    zipkinSpanHandler = (H) alwaysReportSpans(zipkinSpanHandler);
    tracing = Tracing.newBuilder()
      .localServiceName("Aa")
      .localIp("1.2.3.4")
      .localPort(80)
      .addSpanHandler(zipkinSpanHandler)
      .alwaysSampleLocal()
      .build();

    brave.Span unsampledRemote =
      tracing.tracer().nextSpan(TraceContextOrSamplingFlags.NOT_SAMPLED).name("test").start(1L);
    assertThat(unsampledRemote.isNoop()).isFalse();
    assertThat(unsampledRemote.context().sampled()).isFalse();
    assertThat(unsampledRemote.context().sampledLocal()).isTrue();
    unsampledRemote.finish(2L);

    triggerReport();

    // alwaysReportSpans applied
    assertThat(spans).isNotEmpty();
  }

  /** This mainly shows endpoints are taken from Brave, and error is back-filled. */
  @Test void basicSpan() {
    TraceContext context = B3SingleFormat.parseB3SingleFormat(
      "50d980fffa300f29-86154a4ba6e91385-1"
    ).context();

    tracing.tracer().toSpan(context).name("test")
      .start(1L)
      .error(new RuntimeException("this cake is a lie"))
      .finish(3L);

    triggerReport();

    assertThat(spans.get(0)).hasToString(
      "{\"traceId\":\"50d980fffa300f29\","
        + "\"id\":\"86154a4ba6e91385\","
        + "\"name\":\"test\","
        + "\"timestamp\":1,"
        + "\"duration\":2,"
        + "\"localEndpoint\":{"
        + "\"serviceName\":\"aa\","
        + "\"ipv4\":\"1.2.3.4\","
        + "\"port\":80},"
        + "\"tags\":{\"error\":\"this cake is a lie\"}}"
    );
  }

  void triggerReport() {

  }

  /** This shows that in practice, we don't report when the user tells us not to! */
  @Test void abandonedSpan() {
    TraceContext context = B3SingleFormat.parseB3SingleFormat(
      "50d980fffa300f29-86154a4ba6e91385-1"
    ).context();

    tracing.tracer().toSpan(context).name("test")
      .start(1L)
      .abandon(); // whoops.. don't need this one!

    triggerReport();

    assertThat(spans).isEmpty();
  }

  @Test void unsampledSpan() {
    brave.Span unsampledRemote =
      tracing.tracer().nextSpan(TraceContextOrSamplingFlags.NOT_SAMPLED).name("test").start(1L);
    assertThat(unsampledRemote.isNoop()).isTrue();
    assertThat(unsampledRemote.context().sampled()).isFalse();
    assertThat(unsampledRemote.context().sampledLocal()).isFalse();
    unsampledRemote.finish(2L);

    triggerReport();

    assertThat(spans).isEmpty();
  }
}
