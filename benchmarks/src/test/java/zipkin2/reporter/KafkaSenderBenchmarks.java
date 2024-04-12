/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin2.reporter;

import java.util.Collections;
import java.util.Iterator;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.InternetProtocol;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import zipkin2.reporter.internal.SenderBenchmarks;
import zipkin2.reporter.kafka.KafkaSender;

import static org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.testcontainers.utility.DockerImageName.parse;

public class KafkaSenderBenchmarks extends SenderBenchmarks {
  static final Logger LOGGER = LoggerFactory.getLogger(KafkaContainer.class);
  static final int KAFKA_PORT = 19092;

  static final class KafkaContainer extends GenericContainer<KafkaContainer> {
    KafkaContainer() {
      super(parse("ghcr.io/openzipkin/zipkin-kafka:3.1.1"));
      waitStrategy = Wait.forHealthcheck();
      // Kafka broker listener port (19092) needs to be exposed for test cases to access it.
      addFixedExposedPort(KAFKA_PORT, KAFKA_PORT, InternetProtocol.TCP);
      withLogConsumer(new Slf4jLogConsumer(LOGGER));
    }

    String bootstrapServer() {
      return getHost() + ":" + getMappedPort(KAFKA_PORT);
    }
  }

  KafkaContainer kafka;
  KafkaConsumer<byte[], byte[]> consumer;

  @Override protected BytesMessageSender createSender() {
    kafka = new KafkaContainer();
    kafka.start();

    Properties config = new Properties();
    config.put(BOOTSTRAP_SERVERS_CONFIG, kafka.bootstrapServer());
    config.put(GROUP_ID_CONFIG, "zipkin");

    consumer =
      new KafkaConsumer<>(config, new ByteArrayDeserializer(), new ByteArrayDeserializer());
    consumer.subscribe(Collections.singletonList("zipkin"));

    new Thread(() -> {
      while (true) {
        Iterator<ConsumerRecord<byte[], byte[]>> messages = consumer.poll(1000L).iterator();
        while (messages.hasNext()) {
          messages.next();
        }
      }
    }).start();

    return KafkaSender.create(kafka.bootstrapServer());
  }

  @Override protected void afterSenderClose() {
    kafka.stop();
  }

  // Convenience main entry-point
  public static void main(String[] args) throws RunnerException {
    Options opt = new OptionsBuilder()
        .include(".*" + KafkaSenderBenchmarks.class.getSimpleName() + ".*")
        .build();

    new Runner(opt).run();
  }
}

