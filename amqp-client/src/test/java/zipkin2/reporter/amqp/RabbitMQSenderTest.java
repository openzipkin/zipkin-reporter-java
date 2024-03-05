/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin2.reporter.amqp;

import org.junit.jupiter.api.Test;
import zipkin2.reporter.AsyncReporter;
import zipkin2.reporter.BytesMessageSender;
import zipkin2.reporter.ClosedSenderException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static zipkin2.TestObjects.CLIENT_SPAN;
import static zipkin2.reporter.amqp.ITRabbitMQSender.send;

class RabbitMQSenderTest {
  // We can be pretty certain RabbitMQ isn't running on localhost port 80
  RabbitMQSender sender = RabbitMQSender.newBuilder()
    .connectionTimeout(100).addresses("localhost:80").build();

  @Test void sendFailsWhenRabbitMQIsDown() {
    // We can be pretty certain RabbitMQ isn't running on localhost port 80
    RabbitMQSender sender = RabbitMQSender.newBuilder()
      .connectionTimeout(100).addresses("localhost:80").build();

    assertThatThrownBy(() -> send(sender, CLIENT_SPAN, CLIENT_SPAN))
      .isInstanceOf(RuntimeException.class)
      .hasMessageContaining("Unable to establish connection to RabbitMQ server");
  }

  @Test void illegalToSendWhenClosed() throws Exception {
    sender.close();

    assertThatThrownBy(() -> send(sender, CLIENT_SPAN, CLIENT_SPAN))
      .isInstanceOf(ClosedSenderException.class);
  }

  /**
   * The output of toString() on {@link BytesMessageSender} implementations appears in thread names
   * created by {@link AsyncReporter}. Since thread names are likely to be exposed in logs and other
   * monitoring tools, care should be taken to ensure the toString() output is a reasonable length
   * and does not contain sensitive information.
   */
  @Test void toStringContainsOnlySummaryInformation() {
    assertThat(sender).hasToString(
      "RabbitMQSender{addresses=[localhost:80], queue=zipkin}"
    );
  }
}
