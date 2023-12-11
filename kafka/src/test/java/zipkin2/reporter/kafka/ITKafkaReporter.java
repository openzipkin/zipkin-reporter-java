/*
 * Copyright 2016-2019 The OpenZipkin Authors
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
package zipkin2.reporter.kafka;

import com.github.charithe.kafka.EphemeralKafkaBroker;
import com.github.charithe.kafka.KafkaJunitRule;
import java.lang.management.ManagementFactory;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import javax.management.ObjectName;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import zipkin2.CheckResult;
import zipkin2.Span;
import zipkin2.codec.Encoding;
import zipkin2.codec.SpanBytesDecoder;
import zipkin2.reporter.AsyncReporter;
import zipkin2.reporter.Sender;

import static org.assertj.core.api.Assertions.assertThat;
import static zipkin2.TestObjects.CLIENT_SPAN;

public class ITKafkaReporter {
  EphemeralKafkaBroker broker = EphemeralKafkaBroker.create();
  @Rule public KafkaJunitRule kafka = new KafkaJunitRule(broker).waitForStartup();
  @Rule public ExpectedException thrown = ExpectedException.none();

  KafkaReporter reporter;

  @Before public void open() {
    reporter = KafkaReporter.create(broker.getBrokerList().get());
  }

  @After public void close() {
    reporter.close();
  }

  @Test
  public void sendsSpans() throws Exception {
    send(CLIENT_SPAN, CLIENT_SPAN);

    assertThat((readMessage(2)))
      .extracting(SpanBytesDecoder.JSON_V2::decodeOne)
      .containsExactly(CLIENT_SPAN, CLIENT_SPAN);
  }

  @Test
  public void sendsSpans_PROTO3() throws Exception {
    reporter.close();
    reporter = reporter.toBuilder().encoding(Encoding.PROTO3).build();

    send(CLIENT_SPAN, CLIENT_SPAN);

    assertThat((readMessage(2)))
      .extracting(SpanBytesDecoder.PROTO3::decodeOne)
      .containsExactly(CLIENT_SPAN, CLIENT_SPAN);
  }

  @Test
  public void sendsSpans_THRIFT() throws Exception {
    reporter.close();
    reporter = reporter.toBuilder().encoding(Encoding.THRIFT).build();

    send(CLIENT_SPAN, CLIENT_SPAN);

    assertThat((readMessage(2)))
      .extracting(SpanBytesDecoder.THRIFT::decodeOne)
      .containsExactly(CLIENT_SPAN, CLIENT_SPAN);
  }

  @Test
  public void sendsSpansToCorrectTopic() throws Exception {
    reporter.close();
    reporter = reporter.toBuilder().topic("customzipkintopic").build();

    send(CLIENT_SPAN, CLIENT_SPAN);

    assertThat((readMessage("customzipkintopic", 2)))
      .extracting(SpanBytesDecoder.JSON_V2::decodeOne)
      .containsExactly(CLIENT_SPAN, CLIENT_SPAN);
  }

  @Test
  public void checkFalseWhenKafkaIsDown() throws Exception {
    broker.stop();

    // Make a new tracer that fails faster than 60 seconds
    reporter.close();
    Map<String, String> overrides = new LinkedHashMap<>();
    overrides.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "100");
    reporter = reporter.toBuilder().overrides(overrides).build();

    CheckResult check = reporter.check();
    assertThat(check.ok()).isFalse();
    assertThat(check.error()).isInstanceOf(TimeoutException.class);
  }

  @Test
  public void illegalToSendWhenClosed() throws Exception {
    thrown.expect(IllegalStateException.class);
    reporter.close();

    send(CLIENT_SPAN, CLIENT_SPAN);
  }

  @Test
  public void shouldCloseKafkaProducerOnClose() throws Exception {
    send(CLIENT_SPAN, CLIENT_SPAN);

    final ObjectName kafkaProducerMXBeanName = new ObjectName("kafka.producer:*");
    final Set<ObjectName> withProducers = ManagementFactory.getPlatformMBeanServer().queryNames(
        kafkaProducerMXBeanName, null);
    assertThat(withProducers).isNotEmpty();

    reporter.close();

    final Set<ObjectName> withNoProducers = ManagementFactory.getPlatformMBeanServer().queryNames(
        kafkaProducerMXBeanName, null);
    assertThat(withNoProducers).isEmpty();
  }

  @Test
  public void shouldFailWhenMessageIsBiggerThanMaxSize() throws Exception {
    thrown.expect(RecordTooLargeException.class);
    reporter.close();
    reporter = reporter.toBuilder().messageMaxBytes(1).build();

    send(CLIENT_SPAN, CLIENT_SPAN);
    reporter.flush();
  }

  /**
   * The output of toString() on {@link Sender} implementations appears in thread names created by
   * {@link AsyncReporter}. Since thread names are likely to be exposed in logs and other monitoring
   * tools, care should be taken to ensure the toString() output is a reasonable length and does not
   * contain sensitive information.
   */
  @Test
  public void toStringContainsOnlySummaryInformation() {
    assertThat(reporter.toString()).isEqualTo(
        "KafkaReporter{bootstrapServers=" + broker.getBrokerList().get() + ", topic=zipkin-span}"
    );
  }

  void send(Span... spans) {
    for (Span span: spans) reporter.report(span);
  }

  private List<byte[]> readMessage(String topic, int numMessagesToConsume) throws Exception {
    KafkaConsumer<byte[], byte[]> consumer = kafka.helper().createByteConsumer();
    return kafka.helper().consume(topic, consumer, numMessagesToConsume)
        .get().stream().map(ConsumerRecord::value).collect(Collectors.toList());
  }

  private List<byte[]> readMessage(int numMessagesToConsume) throws Exception {
    return readMessage("zipkin-span", numMessagesToConsume);
  }

  private byte[] readMessage() throws Exception {
    return readMessage("zipkin-span", 1).get(0);
  }
}
