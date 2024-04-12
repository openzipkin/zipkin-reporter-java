/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin2.reporter;

import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.SessionProtocol;
import com.linecorp.armeria.server.Route;
import com.linecorp.armeria.server.Server;
import java.time.Duration;
import zipkin2.reporter.internal.SenderBenchmarks;

import static com.linecorp.armeria.common.HttpMethod.POST;
import static com.linecorp.armeria.common.MediaType.JSON;

public abstract class HttpSenderBenchmarks extends SenderBenchmarks {
  Server server;

  @Override protected BytesMessageSender createSender() {
    Route v2JsonSpans = Route.builder().methods(POST).consumes(JSON).path("/api/v2/spans").build();
    server = Server.builder()
      .http(0)
      .gracefulShutdownTimeout(Duration.ZERO, Duration.ZERO)
      .service(v2JsonSpans, (ctx, res) -> HttpResponse.of(202)).build();

    server.start().join();
    return newHttpSender(url("/api/v2/spans"));
  }

  abstract BytesMessageSender newHttpSender(String endpoint);

  @Override protected void afterSenderClose() {
    server.stop().join();
  }

  String url(String path) {
    return server.activePorts().values().stream()
      .filter(p -> p.hasProtocol(SessionProtocol.HTTP)).findAny()
      .map(p -> "http://127.0.0.1:" + p.localAddress().getPort() + path)
      .orElseThrow(() -> new AssertionError("http port not open"));
  }
}
