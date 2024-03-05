/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin2.reporter.amqp;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import zipkin2.Span;
import zipkin2.codec.SpanBytesDecoder;
import zipkin2.reporter.BytesMessageSender;
import zipkin2.reporter.Encoding;
import zipkin2.reporter.SpanBytesEncoder;

import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;
import static zipkin2.TestObjects.CLIENT_SPAN;

@Tag("docker")
@Testcontainers(disabledWithoutDocker = true)
@Timeout(60)
public class ITRabbitMQSender { // public for use in src/it
  @Container RabbitMQContainer rabbit = new RabbitMQContainer();

  @Test void emptyOk() throws Exception {
    try (RabbitMQSender sender = rabbit.newSenderBuilder("emptyOk").build()) {
      sender.send(Collections.emptyList());
    }
  }

  @Test void send() throws Exception {
    try (RabbitMQSender sender = rabbit.newSenderBuilder("send").build()) {

      send(sender, CLIENT_SPAN, CLIENT_SPAN);

      assertThat(SpanBytesDecoder.JSON_V2.decodeList(readMessage(sender)))
        .containsExactly(CLIENT_SPAN, CLIENT_SPAN);
    }
  }

  @Test void send_PROTO3() throws Exception {
    try (RabbitMQSender sender = rabbit.newSenderBuilder("send_PROTO3")
      .encoding(Encoding.PROTO3)
      .build()) {

      send(sender, CLIENT_SPAN, CLIENT_SPAN);

      assertThat(SpanBytesDecoder.PROTO3.decodeList(readMessage(sender)))
        .containsExactly(CLIENT_SPAN, CLIENT_SPAN);
    }
  }

  @Test void send_configuredQueueDoesntExist() throws Exception {
    try (RabbitMQSender sender = rabbit.newSenderBuilder("ignored")
      .queue("send_configuredQueueDoesntExist")
      .build()) {

      send(sender, CLIENT_SPAN, CLIENT_SPAN); // doesn't raise exception
    }
  }

  @Test void shouldCloseRabbitMQConnectionOnClose() throws Exception {
    try (RabbitMQSender sender = rabbit.newSenderBuilder("shouldCloseRabbitMQConnectionOnClose")
      .build()) {

      send(sender, CLIENT_SPAN, CLIENT_SPAN);

      sender.close();

      assertThat(sender.connection.isOpen())
        .isFalse();
    }
  }

  /** Blocks until the callback completes to allow read-your-writes consistency during tests. */
  static void send(BytesMessageSender sender, Span... spans) throws IOException {
    SpanBytesEncoder bytesEncoder = sender.encoding() == Encoding.JSON
      ? SpanBytesEncoder.JSON_V2 : SpanBytesEncoder.PROTO3;
    sender.send(Stream.of(spans).map(bytesEncoder::encode).collect(toList()));
  }

  byte[] readMessage(RabbitMQSender sender) throws Exception {
    final CountDownLatch countDown = new CountDownLatch(1);
    final AtomicReference<byte[]> result = new AtomicReference<>();

    // Don't close this as it invalidates the sender's connection!
    Channel channel = sender.localChannel();
    channel.basicConsume(sender.queue, true, new DefaultConsumer(channel) {
      @Override public void handleDelivery(String consumerTag, Envelope envelope,
        AMQP.BasicProperties properties, byte[] body) {
        result.set(body);
        countDown.countDown();
      }
    });
    assertThat(countDown.await(10, TimeUnit.SECONDS))
      .withFailMessage("Timed out waiting to read message")
      .isTrue();
    assertThat(result)
      .withFailMessage("handleDelivery set null body")
      .isNotNull();
    return result.get();
  }
}
