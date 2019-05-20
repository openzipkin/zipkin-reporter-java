/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package zipkin2.reporter.kafka08;

import com.github.charithe.kafka.KafkaJunitRule;
import java.lang.management.ManagementFactory;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;
import javax.management.ObjectName;
import kafka.serializer.DefaultDecoder;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import zipkin2.Call;
import zipkin2.CheckResult;
import zipkin2.Span;
import zipkin2.codec.Encoding;
import zipkin2.codec.SpanBytesDecoder;
import zipkin2.codec.SpanBytesEncoder;
import zipkin2.reporter.AsyncReporter;
import zipkin2.reporter.Sender;

import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;
import static zipkin2.TestObjects.CLIENT_SPAN;

public class ITKafkaSender {

  @Rule public KafkaJunitRule kafka = new KafkaJunitRule();
  @Rule public ExpectedException thrown = ExpectedException.none();

  String bootstrapServers = "localhost:" + kafka.kafkaBrokerPort();
  KafkaSender sender = KafkaSender.create(bootstrapServers);

  @After
  public void close() {
    sender.close();
  }

  @Test
  public void sendsSpans() throws Exception {
    send(CLIENT_SPAN, CLIENT_SPAN).execute();

    assertThat(SpanBytesDecoder.JSON_V2.decodeList(readMessages().get(0)))
        .containsExactly(CLIENT_SPAN, CLIENT_SPAN);
  }

  @Test
  public void sendsSpans_PROTO3() throws Exception {
    sender.close();
    sender = sender.toBuilder().encoding(Encoding.PROTO3).build();

    send(CLIENT_SPAN, CLIENT_SPAN).execute();

    assertThat(SpanBytesDecoder.PROTO3.decodeList(readMessages().get(0)))
        .containsExactly(CLIENT_SPAN, CLIENT_SPAN);
  }

  @Test
  public void sendsSpans_THRIFT() throws Exception {
    sender.close();
    sender = sender.toBuilder().encoding(Encoding.THRIFT).build();

    send(CLIENT_SPAN, CLIENT_SPAN).execute();

    assertThat(SpanBytesDecoder.THRIFT.decodeList(readMessages().get(0)))
        .containsExactly(CLIENT_SPAN, CLIENT_SPAN);
  }

  @Test
  public void sendsSpansToCorrectTopic() throws Exception {
    sender.close();
    sender = sender.toBuilder().topic("customzipkintopic").build();

    send(CLIENT_SPAN, CLIENT_SPAN).execute();

    assertThat(SpanBytesDecoder.JSON_V2.decodeList(readMessages("customzipkintopic").get(0)))
        .containsExactly(CLIENT_SPAN, CLIENT_SPAN);
  }

  @Test
  public void checkFalseWhenKafkaIsDown() {
    kafka.shutdownKafka();

    // Make a new tracer that fails faster than 60 seconds
    sender.close();
    Map<String, String> overrides = new LinkedHashMap<>();
    overrides.put(ProducerConfig.METADATA_FETCH_TIMEOUT_CONFIG, "100");
    sender = sender.toBuilder().overrides(overrides).build();

    CheckResult check = sender.check();
    assertThat(check.ok()).isFalse();
    assertThat(check.error()).isInstanceOf(org.apache.kafka.common.errors.TimeoutException.class);
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
    final Set<ObjectName> withProducers =
        ManagementFactory.getPlatformMBeanServer().queryNames(kafkaProducerMXBeanName, null);
    assertThat(withProducers).isNotEmpty();

    sender.close();

    final Set<ObjectName> withNoProducers =
        ManagementFactory.getPlatformMBeanServer().queryNames(kafkaProducerMXBeanName, null);
    assertThat(withNoProducers).isEmpty();
  }

  /**
   * The output of toString() on {@link Sender} implementations appears in thread names created by
   * {@link AsyncReporter}. Since thread names are likely to be exposed in logs and other monitoring
   * tools, care should be taken to ensure the toString() output is a reasonable length and does not
   * contain sensitive information.
   */
  @Test
  public void toStringContainsOnlySummaryInformation() {
    assertThat(sender.toString())
        .isEqualTo("KafkaSender{bootstrapServers=" + bootstrapServers + ", topic=zipkin}");
  }

  Call<Void> send(Span... spans) {
    SpanBytesEncoder bytesEncoder;
    switch (sender.encoding()) {
      case JSON:
        bytesEncoder = SpanBytesEncoder.JSON_V2;
        break;
      case THRIFT:
        bytesEncoder = SpanBytesEncoder.THRIFT;
        break;
      case PROTO3:
        bytesEncoder = SpanBytesEncoder.PROTO3;
        break;
      default:
        throw new UnsupportedOperationException("encoding: " + sender.encoding());
    }
    return sender.sendSpans(Stream.of(spans).map(bytesEncoder::encode).collect(toList()));
  }

  private List<byte[]> readMessages(String topic) throws TimeoutException {
    return kafka.readMessages(topic, 1, new DefaultDecoder(kafka.consumerConfig().props()));
  }

  private List<byte[]> readMessages() throws TimeoutException {
    return readMessages("zipkin");
  }
}
