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
package zipkin2.reporter.brave.no_zipkin_deps;

import brave.handler.MutableSpan;
import brave.handler.SpanHandler;
import brave.propagation.B3SingleFormat;
import brave.propagation.TraceContext;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import zipkin2.reporter.brave.AsyncZipkinSpanHandler;
import zipkin2.reporter.okhttp3.OkHttpSender;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

class AsyncZipkinSpanHandlerTest {
  MockWebServer server = new MockWebServer();

  @AfterEach void closeServer() throws IOException {
    server.close();
  }

  String endpoint = server.url("/api/v2/spans").toString();
  OkHttpSender sender =
    OkHttpSender.newBuilder().endpoint(endpoint).compressionEnabled(false).build();

  @Test void sendsSpans() throws Exception {
    server.enqueue(new MockResponse());

    try (AsyncZipkinSpanHandler zipkinSpanHandler = AsyncZipkinSpanHandler.newBuilder(sender)
      .messageTimeout(0, TimeUnit.MILLISECONDS) // don't spawn a thread
      .build()) {

      TraceContext context =
        B3SingleFormat.parseB3SingleFormat("50d980fffa300f29-86154a4ba6e91385-1").context();

      MutableSpan span = new MutableSpan(context, null);

      span.localServiceName("Aa");
      span.localIp("1.2.3.4");
      span.localPort(80);

      span.name("test");
      span.startTimestamp(1L);
      span.error(new RuntimeException("this cake is a lie"));
      span.finishTimestamp(3L);

      zipkinSpanHandler.begin(context, span, null);
      zipkinSpanHandler.end(context, span, SpanHandler.Cause.FINISHED);
      zipkinSpanHandler.flush();

      assertThat(server.takeRequest().getBody().readString(UTF_8)).isEqualTo(""
        + "[{\"traceId\":\"50d980fffa300f29\","
        + "\"id\":\"86154a4ba6e91385\","
        + "\"name\":\"test\","
        + "\"timestamp\":1,"
        + "\"duration\":2,"
        + "\"localEndpoint\":{"
        + "\"serviceName\":\"Aa\","
        + "\"ipv4\":\"1.2.3.4\","
        + "\"port\":80},"
        + "\"tags\":{\"error\":\"this cake is a lie\"}}]");
    }
  }
}
