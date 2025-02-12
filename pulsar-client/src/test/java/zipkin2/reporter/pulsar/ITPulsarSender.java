/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin2.reporter.pulsar;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import zipkin2.Span;
import zipkin2.codec.SpanBytesDecoder;
import zipkin2.reporter.AsyncReporter;
import zipkin2.reporter.BytesMessageSender;
import zipkin2.reporter.Encoding;
import zipkin2.reporter.SpanBytesEncoder;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.HashMap;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static zipkin2.TestObjects.CLIENT_SPAN;

@Tag("docker")
@Testcontainers(disabledWithoutDocker = true)
@Timeout(60)
class ITPulsarSender {

  @Container
  PulsarContainer pulsar = new PulsarContainer();
  String testName;

  @BeforeEach void start(TestInfo testInfo) throws Exception {
    Optional<Method> testMethod = testInfo.getTestMethod();
    testMethod.ifPresent(method -> this.testName = method.getName());
    Thread.sleep(2000); // wait for pulsar to start completely
  }

  @Test void emptyOk() throws IOException {
    try (PulsarSender sender = pulsar.newSenderBuilder(testName).build()) {
      sender.send(Collections.emptyList());
    }
  }

  @Test void sendFailsWithInvalidPulsarServer() throws IOException {
    try (PulsarSender sender = PulsarSender.create("@zixin")) {
      assertThatThrownBy(() -> send(sender, CLIENT_SPAN, CLIENT_SPAN))
          .isInstanceOf(RuntimeException.class)
          .hasMessageContaining("Pulsar client creation failed.");
    }
  }

  @Test void send_JSON() throws Exception {
    try (PulsarSender sender = pulsar.newSenderBuilder(testName)
        .encoding(Encoding.JSON)
        .build()) {
      send(sender, CLIENT_SPAN, CLIENT_SPAN);

      assertThat(SpanBytesDecoder.JSON_V2.decodeList(readMessage(sender))).containsExactly(
          CLIENT_SPAN, CLIENT_SPAN);
    }
  }

  @Test void send_PROTO3() throws Exception {
    try (PulsarSender sender = pulsar.newSenderBuilder(testName)
        .encoding(Encoding.PROTO3)
        .build()) {
      send(sender, CLIENT_SPAN, CLIENT_SPAN);

      assertThat(SpanBytesDecoder.PROTO3.decodeList(readMessage(sender))).containsExactly(
          CLIENT_SPAN, CLIENT_SPAN);
    }
  }

  @Test void send_THRIFT() throws Exception {
    try (PulsarSender sender = pulsar.newSenderBuilder(testName)
        .encoding(Encoding.THRIFT)
        .build()) {
      send(sender, CLIENT_SPAN, CLIENT_SPAN);

      assertThat(SpanBytesDecoder.THRIFT.decodeList(readMessage(sender))).containsExactly(
          CLIENT_SPAN, CLIENT_SPAN);
    }
  }

  @Test void illegalToSendWhenClosed() throws IOException {
    try (PulsarSender sender = pulsar.newSenderBuilder(testName).build()) {
      sender.close();
      assertThatThrownBy(() -> send(sender, CLIENT_SPAN, CLIENT_SPAN)).isInstanceOf(
          IllegalStateException.class);
    }
  }

  /**
   * The output of toString() on {@link BytesMessageSender} implementations appears in thread names
   * created by {@link AsyncReporter}. Since thread names are likely to be exposed in logs and other
   * monitoring tools, care should be taken to ensure the toString() output is a reasonable length
   * and does not contain sensitive information.
   */
  @Test void toStringContainsOnlySummaryInformation() throws IOException {
    try (PulsarSender sender = pulsar.newSenderBuilder(testName).build()) {
      assertThat(sender).hasToString(String.format(
          "PulsarSender{clientProps={serviceUrl=%s}, producerProps=%s, messageProps=%s, topic=%s}",
          pulsar.serviceUrl(),
          new HashMap<>(),
          new HashMap<>(),
          testName));
    }
  }

  void send(PulsarSender sender, Span... spans) {
    SpanBytesEncoder bytesEncoder;
    switch (sender.encoding()) {
      case JSON:
        bytesEncoder = SpanBytesEncoder.JSON_V2;
        break;
      case THRIFT:
        bytesEncoder = SpanBytesEncoder.THRIFT;
        break;
      case PROTO3:
        bytesEncoder = SpanBytesEncoder.PROTO3;
        break;
      default:
        throw new UnsupportedOperationException("encoding: " + sender.encoding());
    }
    sender.send(Stream.of(spans).map(bytesEncoder::encode).collect(toList()));
  }

  byte[] readMessage(PulsarSender sender) throws Exception {
    final CountDownLatch countDown = new CountDownLatch(1);
    final AtomicReference<byte[]> result = new AtomicReference<>();

    try (Consumer<byte[]> ignored = sender.client.newConsumer()
        .topic(sender.topic)
        .subscriptionName("zipkin-subscription")
        .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
        .messageListener((consumer, message) -> {
          try {
            result.set(message.getData());
            countDown.countDown();
            consumer.acknowledge(message);
          } catch (Exception e) {
            consumer.negativeAcknowledge(message);
          }
        }).subscribe()) {

      assertThat(countDown.await(10, TimeUnit.SECONDS))
          .withFailMessage("Timed out waiting to read message.")
          .isTrue();
      assertThat(result)
          .withFailMessage("Message data is null in Pulsar consumer.")
          .isNotNull();
      return result.get();
    }
  }
}
