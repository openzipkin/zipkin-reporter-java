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
package zipkin2.reporter.activemq;

import java.io.IOException;
import java.util.stream.Stream;
import javax.jms.BytesMessage;
import javax.jms.MessageConsumer;
import javax.jms.Queue;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;
import zipkin2.Call;
import zipkin2.CheckResult;
import zipkin2.Span;
import zipkin2.codec.Encoding;
import zipkin2.codec.SpanBytesDecoder;
import zipkin2.codec.SpanBytesEncoder;
import zipkin2.reporter.AsyncReporter;
import zipkin2.reporter.Sender;

import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static zipkin2.TestObjects.CLIENT_SPAN;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Timeout(60)
class ITActiveMQSender {
  @RegisterExtension ActiveMQExtension activemq = new ActiveMQExtension();

  @Test void checkPasses() {
    try (ActiveMQSender sender = activemq.newSenderBuilder("checkPasses").build()) {
      assertThat(sender.check().ok()).isTrue();
    }
  }

  @Test void checkFalseWhenBrokerIsDown() {
    // we can be pretty certain ActiveMQ isn't running on localhost port 80
    try (ActiveMQSender sender = ActiveMQSender.create("tcp://localhost:80")) {
      CheckResult check = sender.check();
      assertThat(check.ok()).isFalse();
      assertThat(check.error()).isInstanceOf(IOException.class);
    }
  }

  @Test void sendFailsWithInvalidActiveMqServer() {
    // we can be pretty certain ActiveMQ isn't running on localhost port 80
    try (ActiveMQSender sender = ActiveMQSender.create("tcp://localhost:80")) {
      assertThatThrownBy(() -> send(sender, CLIENT_SPAN, CLIENT_SPAN).execute()).isInstanceOf(
          IOException.class)
        .hasMessageContaining("Unable to establish connection to ActiveMQ broker");
    }
  }

  @Test void sendsSpans() throws Exception {
    try (ActiveMQSender sender = activemq.newSenderBuilder("sendsSpans").build()) {
      send(sender, CLIENT_SPAN, CLIENT_SPAN).execute();

      assertThat(SpanBytesDecoder.JSON_V2.decodeList(readMessage(sender))).containsExactly(
        CLIENT_SPAN, CLIENT_SPAN);
    }
  }

  @Test void sendsSpans_PROTO3() throws Exception {
    try (ActiveMQSender sender = activemq.newSenderBuilder("sendsSpans_PROTO3")
      .encoding(Encoding.PROTO3)
      .build()) {
      send(sender, CLIENT_SPAN, CLIENT_SPAN).execute();

      assertThat(SpanBytesDecoder.PROTO3.decodeList(readMessage(sender))).containsExactly(
        CLIENT_SPAN, CLIENT_SPAN);
    }
  }

  @Test void sendsSpans_THRIFT() throws Exception {
    try (ActiveMQSender sender = activemq.newSenderBuilder("sendsSpans_THRIFT")
      .encoding(Encoding.THRIFT)
      .build()) {
      send(sender, CLIENT_SPAN, CLIENT_SPAN).execute();

      assertThat(SpanBytesDecoder.THRIFT.decodeList(readMessage(sender))).containsExactly(
        CLIENT_SPAN, CLIENT_SPAN);
    }
  }

  @Test void illegalToSendWhenClosed() {
    try (ActiveMQSender sender = activemq.newSenderBuilder("illegalToSendWhenClosed").build()) {
      sender.close();
      assertThatThrownBy(() -> send(sender, CLIENT_SPAN, CLIENT_SPAN).execute()).isInstanceOf(
        IllegalStateException.class);
    }
  }

  /**
   * The output of toString() on {@link Sender} implementations appears in thread names created by
   * {@link AsyncReporter}. Since thread names are likely to be exposed in logs and other monitoring
   * tools, care should be taken to ensure the toString() output is a reasonable length and does not
   * contain sensitive information.
   */
  @Test void toStringContainsOnlySummaryInformation() {
    try (ActiveMQSender sender = activemq.newSenderBuilder("toString").build()) {
      assertThat(sender).hasToString(
        String.format("ActiveMQSender{brokerURL=%s, queue=toString}", activemq.brokerURL()));
    }
  }

  Call<Void> send(ActiveMQSender sender, Span... spans) {
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
    return sender.sendSpans(Stream.of(spans).map(bytesEncoder::encode).collect(toList()));
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
