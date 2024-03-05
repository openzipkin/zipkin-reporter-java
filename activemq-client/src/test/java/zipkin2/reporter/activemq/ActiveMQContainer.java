/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin2.reporter.activemq;

import java.time.Duration;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;

import static org.testcontainers.utility.DockerImageName.parse;

final class ActiveMQContainer extends GenericContainer<ActiveMQContainer> {
  static final Logger LOGGER = LoggerFactory.getLogger(ActiveMQContainer.class);
  static final int ACTIVEMQ_PORT = 61616;

  ActiveMQContainer() {
    super(parse("ghcr.io/openzipkin/zipkin-activemq:3.0.6"));
    withExposedPorts(ACTIVEMQ_PORT);
    waitStrategy = Wait.forListeningPorts(ACTIVEMQ_PORT);
    withStartupTimeout(Duration.ofSeconds(60));
    withLogConsumer(new Slf4jLogConsumer(LOGGER));
  }

  ActiveMQSender.Builder newSenderBuilder(String queue) {
    ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory();
    connectionFactory.setBrokerURL(brokerURL());
    return ActiveMQSender.newBuilder().queue(queue).connectionFactory(connectionFactory);
  }

  String brokerURL() {
    return "failover:tcp://" + getHost() + ":" + getMappedPort(ACTIVEMQ_PORT);
  }
}
