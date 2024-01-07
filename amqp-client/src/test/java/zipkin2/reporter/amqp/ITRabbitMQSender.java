/*
 * Copyright 2016-2024 The OpenZipkin Authors
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
package zipkin2.reporter.amqp;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;
import zipkin2.Span;
import zipkin2.codec.SpanBytesDecoder;
import zipkin2.reporter.SpanBytesEncoder;
import zipkin2.reporter.Call;
import zipkin2.reporter.Encoding;
import zipkin2.reporter.Sender;

import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;
import static zipkin2.TestObjects.CLIENT_SPAN;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Timeout(60)
public class ITRabbitMQSender { // public for use in src/it
  @RegisterExtension RabbitMQExtension rabbit = new RabbitMQExtension();

  @Test void sendsSpans() throws Exception {
    try (RabbitMQSender sender = rabbit.newSenderBuilder("sendsSpans").build()) {

      send(sender, CLIENT_SPAN, CLIENT_SPAN).execute();

      assertThat(SpanBytesDecoder.JSON_V2.decodeList(readMessage(sender)))
        .containsExactly(CLIENT_SPAN, CLIENT_SPAN);
    }
  }

  @Test void sendsSpans_PROTO3() throws Exception {
    try (RabbitMQSender sender = rabbit.newSenderBuilder("sendsSpans_PROTO3")
      .encoding(Encoding.PROTO3)
      .build()) {

      send(sender, CLIENT_SPAN, CLIENT_SPAN).execute();

      assertThat(SpanBytesDecoder.PROTO3.decodeList(readMessage(sender)))
        .containsExactly(CLIENT_SPAN, CLIENT_SPAN);
    }
  }

  @Test void sendsSpans_configuredQueueDoesntExist() throws Exception {
    try (RabbitMQSender sender = rabbit.newSenderBuilder("ignored")
      .queue("sendsSpans_configuredQueueDoesntExist")
      .build()) {

      send(sender, CLIENT_SPAN, CLIENT_SPAN).execute(); // doesn't raise exception
    }
  }

  @Test void shouldCloseRabbitMQConnectionOnClose() throws Exception {
    try (RabbitMQSender sender = rabbit.newSenderBuilder("shouldCloseRabbitMQConnectionOnClose")
      .build()) {

      send(sender, CLIENT_SPAN, CLIENT_SPAN).execute();

      sender.close();

      assertThat(sender.connection.isOpen())
        .isFalse();
    }
  }

  /** Blocks until the callback completes to allow read-your-writes consistency during tests. */
  static Call<Void> send(Sender sender, Span... spans) {
    SpanBytesEncoder bytesEncoder = sender.encoding() == Encoding.JSON
      ? SpanBytesEncoder.JSON_V2 : SpanBytesEncoder.PROTO3;
    return sender.sendSpans(Stream.of(spans).map(bytesEncoder::encode).collect(toList()));
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
