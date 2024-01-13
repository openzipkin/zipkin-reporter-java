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
package zipkin2.reporter.amqp;

import java.time.Duration;
import org.opentest4j.TestAbortedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;

import static org.testcontainers.utility.DockerImageName.parse;
import static zipkin2.reporter.Call.propagateIfFatal;

final class RabbitMQContainer extends GenericContainer<RabbitMQContainer> {
  static final Logger LOGGER = LoggerFactory.getLogger(RabbitMQContainer.class);
  static final int RABBIT_PORT = 5672;

  RabbitMQContainer() {
    super(parse("ghcr.io/openzipkin/zipkin-rabbitmq:3.0.2"));
    withExposedPorts(RABBIT_PORT);
    waitStrategy = Wait.forLogMessage(".*Server startup complete.*", 1);
    withStartupTimeout(Duration.ofSeconds(60));
    withLogConsumer(new Slf4jLogConsumer(LOGGER));
  }

  RabbitMQSender.Builder newSenderBuilder(String queue) {
    declareQueue(queue);
    return RabbitMQSender.newBuilder().queue(queue).addresses(host() + ":" + port());
  }

  void declareQueue(String queue) {
    ExecResult result;
    try {
      result = execInContainer("amqp-declare-queue", "-q", queue);
    } catch (Throwable e) {
      propagateIfFatal(e);
      throw new TestAbortedException("Couldn't declare queue " + queue + ": " + e.getMessage(), e);
    }
    if (result.getExitCode() != 0) {
      throw new TestAbortedException("Couldn't declare queue " + queue + ": " + result);
    }
  }

  String host() {
    return getHost();
  }

  int port() {
    return getMappedPort(RABBIT_PORT);
  }
}
