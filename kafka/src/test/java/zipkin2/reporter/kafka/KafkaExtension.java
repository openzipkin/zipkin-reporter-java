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
package zipkin2.reporter.kafka;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.errors.TopicExistsException;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.opentest4j.TestAbortedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.InternetProtocol;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;

import static org.apache.kafka.clients.admin.AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.testcontainers.utility.DockerImageName.parse;

class KafkaExtension implements BeforeAllCallback, AfterAllCallback {
  static final Logger LOGGER = LoggerFactory.getLogger(KafkaExtension.class);
  static final int KAFKA_PORT = 19092;

  final KafkaContainer container = new KafkaContainer();

  @Override public void beforeAll(ExtensionContext context) {
    if (context.getRequiredTestClass().getEnclosingClass() != null) {
      // Only run once in outermost scope.
      return;
    }

    container.start();
    LOGGER.info("Using bootstrapServer " + bootstrapServer());
  }

  void prepareTopics(String topics, int partitions) {
    Properties config = new Properties();
    config.put(BOOTSTRAP_SERVERS_CONFIG, bootstrapServer());

    List<NewTopic> newTopics = new ArrayList<>();
    List<String> topicNames = new ArrayList<>();
    for (String topic : topics.split(",")) {
      if ("".equals(topic)) continue;
      newTopics.add(new NewTopic(topic, partitions, (short) 1));
      topicNames.add(topic);
    }

    try (AdminClient adminClient = AdminClient.create(config)) {
      Set<String> existingTopics = adminClient.listTopics().names().get();
      if (existingTopics.contains(topics))
        adminClient.deleteTopics(topicNames).all().get();
      adminClient.createTopics(newTopics).all().get();
    } catch (InterruptedException | ExecutionException e) {
      if (e.getCause() != null && e.getCause() instanceof TopicExistsException) return;
      throw new TestAbortedException(
        "Topics could not be created " + newTopics + ": " + e.getMessage(), e);
    }
  }

  String bootstrapServer() {
    return container.getHost() + ":" + container.getMappedPort(KAFKA_PORT);
  }

  @Override public void afterAll(ExtensionContext context) {
    if (context.getRequiredTestClass().getEnclosingClass() != null) {
      // Only run once in outermost scope.
      return;
    }
    container.stop();
  }

  // mostly waiting for https://github.com/testcontainers/testcontainers-java/issues/3537
  static final class KafkaContainer extends GenericContainer<KafkaContainer> {
    KafkaContainer() {
      super(parse("ghcr.io/openzipkin/zipkin-kafka:2.25.2"));
      if ("true".equals(System.getProperty("docker.skip"))) {
        throw new TestAbortedException("${docker.skip} == true");
      }
      waitStrategy = Wait.forHealthcheck();
      // Kafka broker listener port (19092) needs to be exposed for test cases to access it.
      addFixedExposedPort(KAFKA_PORT, KAFKA_PORT, InternetProtocol.TCP);
      withLogConsumer(new Slf4jLogConsumer(LOGGER));
    }
  }
}
