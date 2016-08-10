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
import java.util.List;
import java.util.concurrent.TimeoutException;
import kafka.serializer.DefaultDecoder;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import zipkin.Codec;
import zipkin.Span;
import zipkin.TestObjects;
import zipkin.internal.CallbackCaptor;

import static org.assertj.core.api.Assertions.assertThat;

public class KafkaReporterTest {

  @Rule
  public KafkaJunitRule kafka = new KafkaJunitRule();
  @Rule
  public ExpectedException thrown = ExpectedException.none();

  KafkaReporter reporter =
      KafkaReporter.builder("localhost:" + kafka.kafkaBrokerPort()).build();

  @After
  public void close() {
    reporter.close();
  }

  @Test
  public void sendsSpans() throws Exception {
    accept(TestObjects.TRACE);

    List<byte[]> messages = readMessages();
    assertThat(messages).hasSize(1);

    assertThat(Codec.THRIFT.readSpans(messages.get(0)))
        .isEqualTo(TestObjects.TRACE);
  }

  @Test
  public void submitsSpansToCorrectTopic() throws Exception {
    reporter.close();
    reporter = KafkaReporter.builder("localhost:" + kafka.kafkaBrokerPort())
        .topic("customzipkintopic").build();

    accept(TestObjects.TRACE);

    List<byte[]> messages = readMessages("customzipkintopic");
    assertThat(messages).hasSize(1);

    assertThat(Codec.THRIFT.readSpans(messages.get(0)))
        .isEqualTo(TestObjects.TRACE);
  }

  /** Blocks until the callback completes to allow read-your-writes consistency during tests. */
  void accept(List<Span> spans) {
    CallbackCaptor<Void> captor = new CallbackCaptor<>();
    reporter.accept(spans, captor);
    captor.get(); // block on result
  }

  private List<byte[]> readMessages(String topic) throws TimeoutException {
    return kafka.readMessages(topic, 1, new DefaultDecoder(kafka.consumerConfig().props()));
  }

  private List<byte[]> readMessages() throws TimeoutException {
    return readMessages("zipkin");
  }
}
