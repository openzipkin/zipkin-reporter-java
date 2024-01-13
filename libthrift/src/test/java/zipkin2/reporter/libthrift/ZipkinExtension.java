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
package zipkin2.reporter.libthrift;

import java.io.IOException;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;

import static org.testcontainers.utility.DockerImageName.parse;

final class ZipkinContainer extends GenericContainer<ZipkinContainer> {
  static final Logger LOGGER = LoggerFactory.getLogger(ZipkinContainer.class);
  static final int SCRIBE_PORT = 9410;
  static final int HTTP_PORT = 9411;

  ZipkinContainer() {
    super(parse("ghcr.io/openzipkin/zipkin:3.0.2"));
    // zipkin-server disables scribe by default.
    withEnv("COLLECTOR_SCRIBE_ENABLED", "true");
    withExposedPorts(SCRIBE_PORT, HTTP_PORT);
    waitStrategy = Wait.forHealthcheck();
    withLogConsumer(new Slf4jLogConsumer(LOGGER));
  }

  String host() {
    return getHost();
  }

  int httpPort() {
    return getMappedPort(HTTP_PORT);
  }

  int scribePort() {
    return getMappedPort(SCRIBE_PORT);
  }

  OkHttpClient client = new OkHttpClient.Builder().followRedirects(true).build();

  Response get(String path) throws IOException {
    return client.newCall(new Request.Builder()
      .url("http://" + host() + ":" + httpPort() + path).build()).execute();
  }
}
