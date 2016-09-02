/**
 * Copyright 2016 The OpenZipkin Authors
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

package zipkin.reporter;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import kafka.admin.AdminUtils;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;
import kafka.utils.ZKStringSerializer$;
import org.I0Itec.zkclient.ZkClient;
import org.apache.curator.test.InstanceSpec;
import org.apache.curator.test.TestingServer;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import zipkin.reporter.kafka08.KafkaSender;

import static kafka.consumer.Consumer.createJavaConsumerConnector;

public class KafkaSenderBenchmarks extends SenderBenchmarks {

  static final int messageMaxBytes = 1000000;

  TestingServer zookeeper;
  KafkaServerStartable kafkaServer;
  ConsumerConnector consumer;

  @Override Sender createSender() throws Exception {
    zookeeper = new TestingServer(true);
    String zookeeperConnect = "127.0.0.1:" + zookeeper.getPort();

    int kafkaPort = InstanceSpec.getRandomPort();
    Properties props = new Properties();
    props.put("advertised.host.name", "127.0.0.1");
    props.put("port", Integer.toString(kafkaPort));
    props.put("broker.id", "1");
    props.put("log.dirs", zookeeper.getTempDirectory().getAbsolutePath());
    props.put("zookeeper.connect", zookeeperConnect);
    kafkaServer = new KafkaServerStartable(new KafkaConfig(props));
    kafkaServer.startup();

    createZipkinTopic(zookeeperConnect);

    consumer = blackholeMessagesInZipkinTopic(zookeeperConnect);

    return KafkaSender.builder()
        .bootstrapServers("127.0.0.1:" + kafkaPort)
        .messageMaxBytes(messageMaxBytes)
        .build();
  }

  @Override void afterSenderClose() throws IOException {
    consumer.shutdown();
    kafkaServer.shutdown();
    zookeeper.close();
    if (Files.exists(zookeeper.getTempDirectory().toPath())) {
      Files.walkFileTree(zookeeper.getTempDirectory().toPath(), new SimpleFileVisitor<Path>() {
        @Override public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
            throws IOException {
          Files.deleteIfExists(file);
          return FileVisitResult.CONTINUE;
        }

        @Override public FileVisitResult postVisitDirectory(Path dir, IOException exc)
            throws IOException {
          Files.deleteIfExists(dir);
          return FileVisitResult.CONTINUE;
        }
      });
    }
  }

  void createZipkinTopic(String zookeeperConnect) {
    ZkClient zk = new ZkClient(zookeeperConnect, 10000, 10000, ZKStringSerializer$.MODULE$);
    try {
      AdminUtils.createTopic(zk, "zipkin", 1, 1, new Properties());
    } finally {
      zk.close();
    }
  }

  ConsumerConnector blackholeMessagesInZipkinTopic(String zookeeperConnect) {
    Properties props = new Properties();
    props.put("zookeeper.connect", zookeeperConnect);
    props.put("fetch.message.max.bytes", String.valueOf(messageMaxBytes));
    props.put("group.id", "zipkin");
    props.put("auto.offset.reset", "smallest");
    ConsumerConnector result = createJavaConsumerConnector(new ConsumerConfig(props));

    Map<String, Integer> topicCountMap = new LinkedHashMap<>();
    topicCountMap.put("zipkin", 1);
    for (KafkaStream<byte[], byte[]> stream : result.createMessageStreams(topicCountMap)
        .get("zipkin")) {
      new Thread(() -> {
        ConsumerIterator<byte[], byte[]> messages = stream.iterator();
        while (messages.hasNext()) {
          messages.next();
        }
      }).start();
    }
    return result;
  }

  // Convenience main entry-point
  public static void main(String[] args) throws RunnerException {
    Options opt = new OptionsBuilder()
        .include(".*" + KafkaSenderBenchmarks.class.getSimpleName() + ".*")
        .build();

    new Runner(opt).run();
  }
}

