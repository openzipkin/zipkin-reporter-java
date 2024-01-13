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

