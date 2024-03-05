/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin2.reporter.amqp;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import java.io.IOException;
import java.util.Collections;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import zipkin2.reporter.BytesMessageSender;
import zipkin2.reporter.internal.SenderBenchmarks;

public class RabbitMQSenderBenchmarks extends SenderBenchmarks {
  private Channel channel;

  @Override protected BytesMessageSender createSender() throws Exception {
    RabbitMQSender sender = RabbitMQSender.newBuilder()
      .queue("zipkin-jmh")
      .addresses("localhost:5672").build();

    // check sender works at all
    sender.send(Collections.emptyList());

    channel = sender.localChannel();
    channel.queueDelete(sender.queue);
    channel.queueDeclare(sender.queue, false, true, true, null);

    Thread.sleep(500L);

    new Thread(() -> {
      try {
        channel.basicConsume(sender.queue, true, new DefaultConsumer(channel));
      } catch (IOException e) {
        e.printStackTrace();
      }
    }).start();

    return sender;
  }

  @Override protected void afterSenderClose() {
    // channel is implicitly closed with the connection
  }

  // Convenience main entry-point
  public static void main(String[] args) throws RunnerException {
    Options opt = new OptionsBuilder()
      .include(".*" + RabbitMQSenderBenchmarks.class.getSimpleName() + ".*")
      .build();

    new Runner(opt).run();
  }
}

