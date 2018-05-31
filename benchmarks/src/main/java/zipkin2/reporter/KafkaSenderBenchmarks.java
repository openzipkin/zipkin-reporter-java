/*
 * Copyright 2016-2018 The OpenZipkin Authors
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
package zipkin2.reporter;

import com.github.charithe.kafka.EphemeralKafkaBroker;
import com.github.charithe.kafka.KafkaJunitRule;
import java.util.Collections;
import java.util.Iterator;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import zipkin2.reporter.kafka11.KafkaSender;

public class KafkaSenderBenchmarks extends SenderBenchmarks {
  EphemeralKafkaBroker broker = EphemeralKafkaBroker.create();
  KafkaJunitRule kafka;
  KafkaConsumer<byte[], byte[]> consumer;

  @Override protected Sender createSender() throws Exception {
    broker.start();
    kafka = new KafkaJunitRule(broker).waitForStartup();
    consumer = kafka.helper().createByteConsumer();
    consumer.subscribe(Collections.singletonList("zipkin"));

    new Thread(() -> {
      while (true) {
        Iterator<ConsumerRecord<byte[], byte[]>> messages = consumer.poll(1000L).iterator();
        while (messages.hasNext()) {
          messages.next();
        }
      }
    }).start();

    return KafkaSender.create(broker.getBrokerList().get());
  }

  @Override protected void afterSenderClose() throws Exception {
    broker.stop();
  }

  // Convenience main entry-point
  public static void main(String[] args) throws RunnerException {
    Options opt = new OptionsBuilder()
        .include(".*" + KafkaSenderBenchmarks.class.getSimpleName() + ".*")
        .build();

    new Runner(opt).run();
  }
}

