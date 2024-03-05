/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin2.reporter.activemq;

import java.io.IOException;
import java.util.Collections;
import java.util.stream.Stream;
import javax.jms.BytesMessage;
import javax.jms.MessageConsumer;
import javax.jms.Queue;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import zipkin2.Span;
import zipkin2.codec.SpanBytesDecoder;
import zipkin2.reporter.AsyncReporter;
import zipkin2.reporter.BytesMessageSender;
import zipkin2.reporter.Encoding;
import zipkin2.reporter.SpanBytesEncoder;

import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static zipkin2.TestObjects.CLIENT_SPAN;

@Tag("docker")
@Testcontainers(disabledWithoutDocker = true)
@Timeout(60)
class ITActiveMQSender {
  @Container ActiveMQContainer activemq = new ActiveMQContainer();

  @Test void emptyOk() throws Exception {
    try (ActiveMQSender sender = activemq.newSenderBuilder("checkPasses").build()) {
      sender.send(Collections.emptyList());
    }
  }

  @Test void sendFailsWithInvalidActiveMqServer() {
    // we can be pretty certain ActiveMQ isn't running on localhost port 80
    try (ActiveMQSender sender = ActiveMQSender.create("tcp://localhost:80")) {
      assertThatThrownBy(() -> send(sender, CLIENT_SPAN, CLIENT_SPAN))
        .isInstanceOf(IOException.class)
        .hasMessageContaining("Unable to establish connection to ActiveMQ broker");
    }
  }

  @Test void send() throws Exception {
    try (ActiveMQSender sender = activemq.newSenderBuilder("send").build()) {
      send(sender, CLIENT_SPAN, CLIENT_SPAN);

      assertThat(SpanBytesDecoder.JSON_V2.decodeList(readMessage(sender))).containsExactly(
        CLIENT_SPAN, CLIENT_SPAN);
    }
  }

  @Test void send_PROTO3() throws Exception {
    try (ActiveMQSender sender = activemq.newSenderBuilder("send_PROTO3")
      .encoding(Encoding.PROTO3)
      .build()) {
      send(sender, CLIENT_SPAN, CLIENT_SPAN);

      assertThat(SpanBytesDecoder.PROTO3.decodeList(readMessage(sender))).containsExactly(
        CLIENT_SPAN, CLIENT_SPAN);
    }
  }

  @Test void send_THRIFT() throws Exception {
    try (ActiveMQSender sender = activemq.newSenderBuilder("send_THRIFT")
      .encoding(Encoding.THRIFT)
      .build()) {
      send(sender, CLIENT_SPAN, CLIENT_SPAN);

      assertThat(SpanBytesDecoder.THRIFT.decodeList(readMessage(sender))).containsExactly(
        CLIENT_SPAN, CLIENT_SPAN);
    }
  }

  @Test void illegalToSendWhenClosed() {
    try (ActiveMQSender sender = activemq.newSenderBuilder("illegalToSendWhenClosed").build()) {
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
  @Test void toStringContainsOnlySummaryInformation() {
    try (ActiveMQSender sender = activemq.newSenderBuilder("toString").build()) {
      assertThat(sender).hasToString(
        String.format("ActiveMQSender{brokerURL=%s, queue=toString}", activemq.brokerURL()));
    }
  }

  void send(ActiveMQSender sender, Span... spans) throws IOException {
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

  byte[] readMessage(ActiveMQSender sender) throws Exception {
    ActiveMQConn conn = sender.lazyInit.get();
    Queue queue = conn.sender.getQueue();
    try (MessageConsumer consumer = conn.session.createConsumer(queue)) {
      BytesMessage message = (BytesMessage) consumer.receive(1000L);
      byte[] result = new byte[(int) message.getBodyLength()];
      message.readBytes(result);
      return result;
    }
  }
}
