/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin2.reporter;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy;
import zipkin2.reporter.internal.SenderBenchmarks;
import zipkin2.reporter.pulsar.PulsarSender;

import java.time.Duration;
import java.util.Collections;

import static org.testcontainers.utility.DockerImageName.parse;

public class PulsarSenderBenchmarks extends SenderBenchmarks {
  static final Logger LOGGER = LoggerFactory.getLogger(PulsarSenderBenchmarks.class);

  static final class PulsarContainer extends GenericContainer<PulsarContainer> {
    static final int BROKER_PORT = 6650;
    static final int BROKER_HTTP_PORT = 8080;

    PulsarContainer() {
      super(parse("apachepulsar/pulsar:4.0.2"));
      withExposedPorts(BROKER_PORT, BROKER_HTTP_PORT);
      String cmd = "/pulsar/bin/apply-config-from-env.py /pulsar/conf/standalone.conf " +
          "&& bin/pulsar standalone " +
          "--no-functions-worker -nss";
      withEnv("PULSAR_MEM", "-Xms512m -Xmx512m -XX:MaxDirectMemorySize=1g"); // limit memory usage
      waitStrategy = new HttpWaitStrategy()
          .forPort(BROKER_HTTP_PORT)
          .forStatusCode(200)
          .forPath("/admin/v2/clusters")
          .withStartupTimeout(Duration.ofSeconds(120));
      withCommand("/bin/bash", "-c", cmd);
      withLogConsumer(new Slf4jLogConsumer(LOGGER));
    }

    String serviceUrl() {
      return "pulsar://" + getHost() + ":" + getMappedPort(BROKER_PORT);
    }
  }

  PulsarContainer pulsar;
  Consumer<byte[]> consumer;

  @Override protected BytesMessageSender createSender() throws PulsarClientException {
    pulsar = new PulsarContainer();
    pulsar.start();

    String topicName = "zipkin";
    PulsarSender sender = PulsarSender.newBuilder().topic(topicName)
        .serviceUrl(pulsar.serviceUrl()).build();

    sender.send(Collections.emptyList());
    try (PulsarClient client = PulsarClient.builder()
        .serviceUrl(pulsar.serviceUrl())
        .build()) {
      consumer = client.newConsumer()
          .topic(topicName)
          .subscriptionType(SubscriptionType.Shared)
          .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
          .subscriptionName(topicName)
          .subscribe();
    }

    return sender;
  }

  @Override protected void afterSenderClose() {
    pulsar.stop();
  }

  // Convenience main entry-point
  public static void main(String[] args) throws RunnerException {
    Options opt = new OptionsBuilder()
        .include(".*" + PulsarSenderBenchmarks.class.getSimpleName() + ".*")
        .build();

    new Runner(opt).run();
  }
}

