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
package zipkin.reporter.kafka08;

import com.github.charithe.kafka.KafkaJunitRule;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import kafka.serializer.DefaultDecoder;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.After;
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

  @Rule public KafkaJunitRule kafka = new KafkaJunitRule();
  @Rule public ExpectedException thrown = ExpectedException.none();

  KafkaSender sender = KafkaSender.create("localhost:" + kafka.kafkaBrokerPort());

  @After
  public void close() throws IOException {
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
    kafka.shutdownKafka();

    // Make a new tracer that fails faster than 60 seconds
    sender.close();
    Map<String, String> overrides = new LinkedHashMap<>();
    overrides.put(ProducerConfig.METADATA_FETCH_TIMEOUT_CONFIG, "100");
    sender = sender.toBuilder().overrides(overrides).build();

    Component.CheckResult check = sender.check();
    assertThat(check.ok).isFalse();
    assertThat(check.exception)
        .isInstanceOf(org.apache.kafka.common.errors.TimeoutException.class);
  }

  /** Blocks until the callback completes to allow read-your-writes consistency during tests. */
  void send(List<Span> spans) {
    AwaitableCallback callback = new AwaitableCallback();
    sender.sendSpans(spans.stream().map(Encoder.THRIFT::encode).collect(toList()), callback);
    callback.await();
  }

  private List<byte[]> readMessages(String topic) throws TimeoutException {
    return kafka.readMessages(topic, 1, new DefaultDecoder(kafka.consumerConfig().props()));
  }

  private List<byte[]> readMessages() throws TimeoutException {
    return readMessages("zipkin");
  }
}
