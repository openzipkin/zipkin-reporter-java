/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
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
    super(parse("ghcr.io/openzipkin/zipkin-rabbitmq:3.4.3"));
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
