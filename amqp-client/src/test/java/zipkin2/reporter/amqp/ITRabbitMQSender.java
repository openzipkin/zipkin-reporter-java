/*
 * Copyright 2016-2020 The OpenZipkin Authors
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
import org.junit.After;
import org.junit.AssumptionViolatedException;
import org.junit.ClassRule;
import org.junit.Test;
import zipkin2.Call;
import zipkin2.Span;
import zipkin2.codec.Encoding;
import zipkin2.codec.SpanBytesDecoder;
import zipkin2.codec.SpanBytesEncoder;
import zipkin2.reporter.Sender;

import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;
import static zipkin2.TestObjects.CLIENT_SPAN;

/** This works against a docker container or throws an {@link AssumptionViolatedException}. */
public class ITRabbitMQSender {
  @ClassRule public static RabbitMQSenderRule rabbit = new RabbitMQSenderRule();
  RabbitMQSender sender = rabbit.tryToInitializeSender(rabbit.newSenderBuilder());

  @After public void after() throws Exception {
    sender.close();
  }

  @Test public void sendsSpans() throws Exception {
    send(CLIENT_SPAN, CLIENT_SPAN).execute();

    assertThat(SpanBytesDecoder.JSON_V2.decodeList(readMessage()))
        .containsExactly(CLIENT_SPAN, CLIENT_SPAN);
  }

  @Test public void sendsSpans_PROTO3() throws Exception {
    sender.close();
    sender = rabbit.tryToInitializeSender(rabbit.newSenderBuilder().encoding(Encoding.PROTO3));

    send(CLIENT_SPAN, CLIENT_SPAN).execute();

    assertThat(SpanBytesDecoder.PROTO3.decodeList(readMessage()))
        .containsExactly(CLIENT_SPAN, CLIENT_SPAN);
  }

  @Test public void sendsSpansToCorrectQueue() throws Exception {
    String differentQueue = "zipkin-test2";

    rabbit.declareQueue(differentQueue);
    sender.close();
    sender = rabbit.tryToInitializeSender(rabbit.newSenderBuilder().queue(differentQueue));

    send(CLIENT_SPAN, CLIENT_SPAN).execute();

    assertThat(SpanBytesDecoder.JSON_V2.decodeList(readMessage()))
        .containsExactly(CLIENT_SPAN, CLIENT_SPAN);
  }

  @Test public void shouldCloseRabbitMQConnectionOnClose() throws Exception {
    send(CLIENT_SPAN, CLIENT_SPAN).execute();

    sender.close();
    assertThat(sender.connection.isOpen())
        .isFalse();
  }

  Call<Void> send(Span... spans) {
    return send(sender, spans);
  }

  /** Blocks until the callback completes to allow read-your-writes consistency during tests. */
  static Call<Void> send(Sender sender, Span... spans) {
    SpanBytesEncoder bytesEncoder = sender.encoding() == Encoding.JSON
        ? SpanBytesEncoder.JSON_V2 : SpanBytesEncoder.PROTO3;
    return sender.sendSpans(Stream.of(spans).map(bytesEncoder::encode).collect(toList()));
  }

  byte[] readMessage() throws Exception {
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
