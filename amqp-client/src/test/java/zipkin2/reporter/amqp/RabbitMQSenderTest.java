/**
 * Copyright 2016-2017 The OpenZipkin Authors
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
import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;
import org.junit.After;
import org.junit.AssumptionViolatedException;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import zipkin2.Call;
import zipkin2.CheckResult;
import zipkin2.Span;
import zipkin2.codec.SpanBytesDecoder;
import zipkin2.codec.SpanBytesEncoder;

import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;
import static zipkin2.reporter.TestObjects.CLIENT_SPAN;

public class RabbitMQSenderTest {
  @Rule public ExpectedException thrown = ExpectedException.none();

  RabbitMQSender sender;

  @Before public void open() throws Exception {
    sender = RabbitMQSender.newBuilder()
        .queue("zipkin-test1")
        .addresses("localhost:5672").build();

    CheckResult check = sender.check();
    if (!check.ok()) {
      throw new AssumptionViolatedException(check.error().getMessage(), check.error());
    }

    declareQueue(sender.queue());
  }

  @After public void close() throws IOException {
    sender.close();
  }

  @Test
  public void sendsSpans() throws Exception {
    send(CLIENT_SPAN, CLIENT_SPAN).execute();

    assertThat(SpanBytesDecoder.JSON_V2.decodeList(readMessage()))
        .containsExactly(CLIENT_SPAN, CLIENT_SPAN);
  }

  @Test
  public void sendsSpansToCorrectQueue() throws Exception {
    sender.close();
    Thread.sleep(100);

    sender = sender.toBuilder().queue("zipkin-test2").build();
    declareQueue(sender.queue());

    send(CLIENT_SPAN, CLIENT_SPAN).execute();

    assertThat(SpanBytesDecoder.JSON_V2.decodeList(readMessage()))
        .containsExactly(CLIENT_SPAN, CLIENT_SPAN);
  }

  @Test
  public void checkFalseWhenRabbitMQIsDown() throws Exception {
    sender.close();
    sender = sender.toBuilder().connectionTimeout(100).addresses("1.2.3.4:1213").build();

    CheckResult check = sender.check();
    assertThat(check.ok()).isFalse();
    assertThat(check.error())
        .isInstanceOf(IllegalStateException.class);
  }

  @Test
  public void illegalToSendWhenClosed() throws Exception {
    thrown.expect(IllegalStateException.class);
    sender.close();

    send(CLIENT_SPAN, CLIENT_SPAN).execute();
  }

  @Test
  public void shouldCloseRabbitMQProducerOnClose() throws Exception {
    send(CLIENT_SPAN, CLIENT_SPAN).execute();

    sender.close();
    assertThat(sender.get().isOpen())
        .isFalse();
  }

  /** Blocks until the callback completes to allow read-your-writes consistency during tests. */
  Call<Void> send(Span... spans) {
    return sender.sendSpans(Stream.of(spans)
        .map(SpanBytesEncoder.JSON_V2::encode)
        .collect(toList()));
  }

  private void declareQueue(String queue) throws Exception {
    Channel channel = sender.get().createChannel();
    try {
      channel.queueDelete(queue);
      channel.queueDeclare(queue, false, true, true, null);
    } finally {
      channel.close();
    }
    Thread.sleep(500L);
  }

  private byte[] readMessage() throws Exception {
    final CountDownLatch countDown = new CountDownLatch(1);
    final AtomicReference<byte[]> result = new AtomicReference<>();

    Channel channel = sender.get().createChannel();
    try {
      channel.basicConsume(sender.queue(), true, new DefaultConsumer(channel) {
        @Override public void handleDelivery(String consumerTag, Envelope envelope,
            AMQP.BasicProperties properties, byte[] body) throws IOException {
          result.set(body);
          countDown.countDown();
        }
      });
      countDown.await(5, TimeUnit.SECONDS);
    } finally {
      channel.close();
    }
    return result.get();
  }
}
