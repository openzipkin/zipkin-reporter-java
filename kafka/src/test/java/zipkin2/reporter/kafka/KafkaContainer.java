/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin2.reporter.kafka;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.errors.TopicExistsException;
import org.opentest4j.TestAbortedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.InternetProtocol;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;

import static org.apache.kafka.clients.admin.AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.testcontainers.utility.DockerImageName.parse;

final class KafkaContainer extends GenericContainer<KafkaContainer> {
  static final Logger LOGGER = LoggerFactory.getLogger(KafkaContainer.class);
  static final int KAFKA_PORT = 19092;

  KafkaContainer() {
    super(parse("ghcr.io/openzipkin/zipkin-kafka:3.4.3"));
    waitStrategy = Wait.forHealthcheck();
    // Kafka broker listener port (19092) needs to be exposed for test cases to access it.
    addFixedExposedPort(KAFKA_PORT, KAFKA_PORT, InternetProtocol.TCP);
    withLogConsumer(new Slf4jLogConsumer(LOGGER));
  }

  void prepareTopics(String topics) {
    Properties config = new Properties();
    config.put(BOOTSTRAP_SERVERS_CONFIG, bootstrapServer());

    List<NewTopic> newTopics = new ArrayList<>();
    List<String> topicNames = new ArrayList<>();
    for (String topic : topics.split(",")) {
      if ("".equals(topic)) continue;
      newTopics.add(new NewTopic(topic, 1, (short) 1));
      topicNames.add(topic);
    }

    try (AdminClient adminClient = AdminClient.create(config)) {
      Set<String> existingTopics = adminClient.listTopics().names().get();
      if (existingTopics.contains(topics)) {
        adminClient.deleteTopics(topicNames).all().get();
      }
      adminClient.createTopics(newTopics).all().get();
    } catch (InterruptedException | ExecutionException e) {
      if (e.getCause() != null && e.getCause() instanceof TopicExistsException) return;
      throw new TestAbortedException(
        "Topics could not be created " + newTopics + ": " + e.getMessage(), e);
    }
  }

  String bootstrapServer() {
    return getHost() + ":" + getMappedPort(KAFKA_PORT);
  }
}
