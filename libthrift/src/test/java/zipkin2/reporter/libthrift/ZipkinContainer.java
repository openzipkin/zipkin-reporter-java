/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
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
    super(parse("ghcr.io/openzipkin/zipkin:3.4.3"));
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
