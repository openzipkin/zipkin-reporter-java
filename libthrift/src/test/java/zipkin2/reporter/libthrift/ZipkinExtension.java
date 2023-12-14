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
package zipkin2.reporter.libthrift;

import java.io.IOException;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.opentest4j.TestAbortedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;

import static org.testcontainers.utility.DockerImageName.parse;

class ZipkinExtension implements BeforeAllCallback, AfterAllCallback {
  static final Logger LOGGER = LoggerFactory.getLogger(ZipkinExtension.class);
  static final int SCRIBE_PORT = 9410;
  static final int HTTP_PORT = 9411;

  final ZipkinContainer container = new ZipkinContainer();

  @Override public void beforeAll(ExtensionContext context) {
    if (context.getRequiredTestClass().getEnclosingClass() != null) {
      // Only run once in outermost scope.
      return;
    }

    container.start();
    LOGGER.info("Using scribe host and port " + host() + ":" + scribePort());
  }

  String host() {
    return container.getHost();
  }

  int httpPort() {
    return container.getMappedPort(HTTP_PORT);
  }

  int scribePort() {
    return container.getMappedPort(SCRIBE_PORT);
  }

  OkHttpClient client = new OkHttpClient.Builder().followRedirects(true).build();

  Response get(String path) throws IOException {
    return client.newCall(new Request.Builder()
      .url("http://" + host() + ":" + httpPort() + path).build()).execute();
  }

  @Override public void afterAll(ExtensionContext context) {
    if (context.getRequiredTestClass().getEnclosingClass() != null) {
      // Only run once in outermost scope.
      return;
    }
    container.stop();
  }

  // mostly waiting for https://github.com/testcontainers/testcontainers-java/issues/3537
  static final class ZipkinContainer extends GenericContainer<ZipkinContainer> {
    ZipkinContainer() {
      super(parse("ghcr.io/openzipkin/zipkin:2.25.1"));
      if ("true".equals(System.getProperty("docker.skip"))) {
        throw new TestAbortedException("${docker.skip} == true");
      }
      // zipkin-server disables scribe by default.
      withEnv("COLLECTOR_SCRIBE_ENABLED", "true");
      withExposedPorts(SCRIBE_PORT, HTTP_PORT);
      waitStrategy = Wait.forHealthcheck();
      withLogConsumer(new Slf4jLogConsumer(LOGGER));
    }
  }
}
