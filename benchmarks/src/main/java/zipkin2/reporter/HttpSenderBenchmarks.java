/*
 * Copyright 2016-2018 The OpenZipkin Authors
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

import io.undertow.Undertow;
import java.io.IOException;
import java.net.InetSocketAddress;

public abstract class HttpSenderBenchmarks extends SenderBenchmarks {
  Undertow server;

  @Override protected Sender createSender() throws Exception {
    server = Undertow.builder()
        .addHttpListener(0, "127.0.0.1")
        .setHandler(exchange -> exchange.setStatusCode(202).endExchange()).build();

    server.start();
    return newHttpSender("http://127.0.0.1:" +
        ((InetSocketAddress) server.getListenerInfo().get(0).getAddress()).getPort()
        + "/api/v1/spans");
  }

  abstract Sender newHttpSender(String endpoint);

  @Override protected void afterSenderClose() throws IOException {
    server.stop();
  }
}
