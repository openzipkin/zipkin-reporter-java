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
package zipkin2.reporter.kafka11;

import com.github.charithe.kafka.EphemeralKafkaBroker;
import com.github.charithe.kafka.KafkaJunitRule;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;
import javax.management.ObjectName;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import zipkin2.Call;
import zipkin2.CheckResult;
import zipkin2.Span;
import zipkin2.codec.SpanBytesDecoder;
import zipkin2.codec.SpanBytesEncoder;
import zipkin2.reporter.AsyncReporter;
import zipkin2.reporter.Sender;

import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;
import static zipkin2.reporter.TestObjects.CLIENT_SPAN;

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
    send(CLIENT_SPAN, CLIENT_SPAN).execute();

    assertThat(SpanBytesDecoder.JSON_V2.decodeList(readMessage()))
        .containsExactly(CLIENT_SPAN, CLIENT_SPAN);
  }

  @Test
  public void sendsSpansToCorrectTopic() throws Exception {
    sender.close();
    sender = sender.toBuilder().topic("customzipkintopic").build();

    send(CLIENT_SPAN, CLIENT_SPAN).execute();

    assertThat(SpanBytesDecoder.JSON_V2.decodeList(readMessage("customzipkintopic")))
        .containsExactly(CLIENT_SPAN, CLIENT_SPAN);
  }

  @Test
  public void checkFalseWhenKafkaIsDown() throws Exception {
    broker.stop();

    // Make a new tracer that fails faster than 60 seconds
    sender.close();
    Map<String, String> overrides = new LinkedHashMap<>();
    overrides.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "100");
    sender = sender.toBuilder().overrides(overrides).build();

    CheckResult check = sender.check();
    assertThat(check.ok()).isFalse();
    assertThat(check.error())
        .isInstanceOf(org.apache.kafka.common.errors.TimeoutException.class);
  }

  @Test
  public void illegalToSendWhenClosed() throws Exception {
    thrown.expect(IllegalStateException.class);
    sender.close();

    send(CLIENT_SPAN, CLIENT_SPAN).execute();
  }

  @Test
  public void shouldCloseKafkaProducerOnClose() throws Exception {
    send(CLIENT_SPAN, CLIENT_SPAN).execute();

    final ObjectName kafkaProducerMXBeanName = new ObjectName("kafka.producer:*");
    final Set<ObjectName> withProducers = ManagementFactory.getPlatformMBeanServer().queryNames(
        kafkaProducerMXBeanName, null);
    assertThat(withProducers).isNotEmpty();

    sender.close();

    final Set<ObjectName> withNoProducers = ManagementFactory.getPlatformMBeanServer().queryNames(
        kafkaProducerMXBeanName, null);
    assertThat(withNoProducers).isEmpty();
  }

  /**
   * The output of toString() on {@link Sender} implementations appears in thread names created by
   * {@link AsyncReporter}. Since thread names are likely to be exposed in logs and other monitoring
   * tools, care should be taken to ensure the toString() output is a reasonable length and does not
   * contain sensitive information.
   */
  @Test
  public void toStringContainsOnlySummaryInformation() throws Exception {
    assertThat(sender.toString()).isEqualTo(
        "KafkaSender{bootstrapServers=" + broker.getBrokerList().get() + ", topic=zipkin}"
    );
  }

  Call<Void> send(Span... spans) {
    return sender.sendSpans(Stream.of(spans)
        .map(SpanBytesEncoder.JSON_V2::encode)
        .collect(toList()));
  }

  private byte[] readMessage(String topic) throws Exception {
    KafkaConsumer<byte[], byte[]> consumer = kafka.helper().createByteConsumer();
    return kafka.helper().consume(topic, consumer, 1)
        .get().stream().map(ConsumerRecord::value).findFirst().get();
  }

  private byte[] readMessage() throws Exception {
    return readMessage("zipkin");
  }
}
