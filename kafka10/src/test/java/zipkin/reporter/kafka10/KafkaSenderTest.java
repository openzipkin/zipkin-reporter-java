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
package zipkin.reporter.kafka10;

import com.github.charithe.kafka.EphemeralKafkaBroker;
import com.github.charithe.kafka.KafkaJunitRule;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import zipkin.Codec;
import zipkin.Component;
import zipkin.Span;
import zipkin.TestObjects;
import zipkin.reporter.Encoder;
import zipkin.reporter.internal.AwaitableCallback;

import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;

public class KafkaSenderTest {
  EphemeralKafkaBroker broker = EphemeralKafkaBroker.create();
  @Rule public KafkaJunitRule kafka = new KafkaJunitRule(broker).waitForStartup();
  @Rule public ExpectedException thrown = ExpectedException.none();

  KafkaSender sender;

  @Before public void open() throws IOException {
    sender = KafkaSender.create(broker.getBrokerList().get());
  }

  @After public void close() throws IOException {
    sender.close();
  }

  @Test
  public void sendsSpans() throws Exception {
    send(TestObjects.TRACE);

    List<byte[]> messages = readMessages();
    assertThat(messages).hasSize(1);

    assertThat(Codec.THRIFT.readSpans(messages.get(0)))
        .isEqualTo(TestObjects.TRACE);
  }

  @Test
  public void sendsSpansToCorrectTopic() throws Exception {
    sender.close();
    sender = sender.toBuilder().topic("customzipkintopic").build();

    send(TestObjects.TRACE);

    List<byte[]> messages = readMessages("customzipkintopic");
    assertThat(messages).hasSize(1);

    assertThat(Codec.THRIFT.readSpans(messages.get(0)))
        .isEqualTo(TestObjects.TRACE);
  }

  @Test
  public void checkFalseWhenKafkaIsDown() throws Exception {
    broker.stop();

    // Make a new tracer that fails faster than 60 seconds
    sender.close();
    Map<String, String> overrides = new LinkedHashMap<>();
    overrides.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "100");
    sender = sender.toBuilder().overrides(overrides).build();

    Component.CheckResult check = sender.check();
    assertThat(check.ok).isFalse();
    assertThat(check.exception)
        .isInstanceOf(org.apache.kafka.common.errors.TimeoutException.class);
  }

  @Test
  public void illegalToSendWhenClosed() throws Exception {
    thrown.expect(IllegalStateException.class);
    sender.close();

    send(TestObjects.TRACE);
  }

  /**
   * The output of toString() on {@link zipkin.reporter.Sender} implementations appears in thread
   * names created by {@link zipkin.reporter.AsyncReporter}. Since thread names are likely to be
   * exposed in logs and other monitoring tools, care should be taken to ensure the toString()
   * output is a reasonable length and does not contain sensitive information.
   */
  @Test
  public void toStringContainsOnlySenderType() throws Exception {
    assertThat(sender.toString()).isEqualTo("KafkaSender");
  }

  /** Blocks until the callback completes to allow read-your-writes consistency during tests. */
  void send(List<Span> spans) {
    AwaitableCallback callback = new AwaitableCallback();
    sender.sendSpans(spans.stream().map(Encoder.THRIFT::encode).collect(toList()), callback);
    callback.await();
  }

  private List<byte[]> readMessages(String topic) throws Exception {
    KafkaConsumer<byte[], byte[]> consumer = kafka.helper().createByteConsumer();
    return kafka.helper().consume(topic, consumer, 1)
        .get().stream().map(ConsumerRecord::value).collect(toList());
  }

  private List<byte[]> readMessages() throws Exception {
    return readMessages("zipkin");
  }
}
