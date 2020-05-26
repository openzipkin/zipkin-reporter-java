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

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import java.io.IOException;
import org.junit.AssumptionViolatedException;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import zipkin2.CheckResult;
import zipkin2.reporter.Sender;
import zipkin2.reporter.SenderBenchmarks;

public class RabbitMQSenderBenchmarks extends SenderBenchmarks {
  private Channel channel;

  @Override protected Sender createSender() throws Exception {
    RabbitMQSender result = RabbitMQSender.newBuilder()
        .queue("zipkin-jmh")
        .addresses("localhost:5672").build();

    CheckResult check = result.check();
    if (!check.ok()) {
      throw new AssumptionViolatedException(check.error().getMessage(), check.error());
    }

    channel = result.localChannel();
    channel.queueDelete(result.queue);
    channel.queueDeclare(result.queue, false, true, true, null);

    Thread.sleep(500L);

    new Thread(() -> {
      try {
        channel.basicConsume(result.queue, true, new DefaultConsumer(channel));
      } catch (IOException e) {
        e.printStackTrace();
      }
    }).start();

    return result;
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

