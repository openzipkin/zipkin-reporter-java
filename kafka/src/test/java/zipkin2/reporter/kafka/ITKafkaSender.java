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
package zipkin2.reporter.kafka;

import java.lang.management.ManagementFactory;
import java.time.Duration;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Stream;
import javax.management.ObjectName;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import zipkin2.Span;
import zipkin2.codec.SpanBytesDecoder;
import zipkin2.reporter.AsyncReporter;
import zipkin2.reporter.BytesMessageSender;
import zipkin2.reporter.Encoding;
import zipkin2.reporter.SpanBytesEncoder;

import static java.util.stream.Collectors.toList;
import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static zipkin2.TestObjects.CLIENT_SPAN;

@Tag("docker")
@Testcontainers(disabledWithoutDocker = true)
@Timeout(60)
class ITKafkaSender {
  @Container KafkaContainer kafka = new KafkaContainer();

  KafkaSender sender;

  @BeforeEach void open() {
    sender = KafkaSender.create(kafka.bootstrapServer());
    kafka.prepareTopics(sender.topic);
  }

  @AfterEach void close() {
    sender.close();
  }

  @Test void send() {
    sendSpans(CLIENT_SPAN, CLIENT_SPAN);
    sender.producer.flush();

    assertThat(SpanBytesDecoder.JSON_V2.decodeList(readMessage()))
      .containsExactly(CLIENT_SPAN, CLIENT_SPAN);
  }

  @Test void send_PROTO3() {
    sender.close();
    sender = sender.toBuilder().encoding(Encoding.PROTO3).build();

    sendSpans(CLIENT_SPAN, CLIENT_SPAN);
    sender.producer.flush();

    assertThat(SpanBytesDecoder.PROTO3.decodeList(readMessage()))
      .containsExactly(CLIENT_SPAN, CLIENT_SPAN);
  }

  @Test void send_THRIFT() {
    sender.close();
    sender = sender.toBuilder().encoding(Encoding.THRIFT).build();

    sendSpans(CLIENT_SPAN, CLIENT_SPAN);
    sender.producer.flush();

    assertThat(SpanBytesDecoder.THRIFT.decodeList(readMessage()))
      .containsExactly(CLIENT_SPAN, CLIENT_SPAN);
  }

  @Test void sendToCorrectTopic() {
    sender.close();
    kafka.prepareTopics("customzipkintopic");
    sender = sender.toBuilder().topic("customzipkintopic").build();

    sendSpans(CLIENT_SPAN, CLIENT_SPAN);
    sender.producer.flush();

    assertThat(SpanBytesDecoder.JSON_V2.decodeList(readMessage("customzipkintopic")))
      .containsExactly(CLIENT_SPAN, CLIENT_SPAN);
  }

  @Test void sendWhenKafkaIsDown() {
    kafka.stop();

    // Make a new tracer that fails faster than 60 seconds
    sender.close();
    Map<String, String> overrides = new LinkedHashMap<>();
    overrides.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "100");
    sender = sender.toBuilder().overrides(overrides).build();

    assertThatThrownBy(() -> sendSpans(CLIENT_SPAN, CLIENT_SPAN))
      .isInstanceOf(TimeoutException.class);
  }

  @Test void illegalToSendWhenClosed() {
    sender.close();

    assertThatThrownBy(() -> sendSpans(CLIENT_SPAN, CLIENT_SPAN))
      .isInstanceOf(IllegalStateException.class);
  }

  @Test void shouldCloseKafkaProducerOnClose() throws Exception {
    sendSpans(CLIENT_SPAN, CLIENT_SPAN);
    sender.producer.flush();

    final ObjectName kafkaProducerMXBeanName = new ObjectName("kafka.producer:*");
    final Set<ObjectName> withProducers = ManagementFactory.getPlatformMBeanServer().queryNames(
      kafkaProducerMXBeanName, null);
    assertThat(withProducers).isNotEmpty();

    sender.close();

    final Set<ObjectName> withNoProducers = ManagementFactory.getPlatformMBeanServer().queryNames(
      kafkaProducerMXBeanName, null);
    assertThat(withNoProducers).isEmpty();
  }

  @Test void shouldFailWhenMessageIsBiggerThanMaxSize() {
    sender.close();
    sender = sender.toBuilder().messageMaxBytes(1).build();

    assertThatThrownBy(() -> sendSpans(CLIENT_SPAN, CLIENT_SPAN))
      .isInstanceOf(RecordTooLargeException.class);
  }

  /**
   * The output of toString() on {@link BytesMessageSender} implementations appears in thread names
   * created by {@link AsyncReporter}. Since thread names are likely to be exposed in logs and other
   * monitoring tools, care should be taken to ensure the toString() output is a reasonable length
   * and does not contain sensitive information.
   */
  @Test void toStringContainsOnlySummaryInformation() {
    assertThat(sender.toString()).isEqualTo(
      "KafkaSender{bootstrapServers=" + kafka.bootstrapServer() + ", topic=zipkin}"
    );
  }

  @Test void checkFilterPropertiesProducerToAdminClient() {
    Properties producerProperties = new Properties();
    producerProperties.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "100");
    producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
      ByteArraySerializer.class.getName());
    producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
      ByteArraySerializer.class.getName());
    producerProperties.put(ProducerConfig.BATCH_SIZE_CONFIG, "0");
    producerProperties.put(ProducerConfig.ACKS_CONFIG, "0");
    producerProperties.put(ProducerConfig.LINGER_MS_CONFIG, "500");
    producerProperties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, "33554432");
    producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
    producerProperties.put(ProducerConfig.SECURITY_PROVIDERS_CONFIG, "sun.security.provider.Sun");

    Map<String, Object> filteredProperties =
      sender.filterPropertiesForAdminClient(producerProperties);

    assertThat(filteredProperties.size()).isEqualTo(2);
    assertThat(filteredProperties.get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG)).isNotNull();
    assertThat(filteredProperties.get(ProducerConfig.SECURITY_PROVIDERS_CONFIG)).isNotNull();
  }

  void sendSpans(Span... spans) {
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
    sender.send(Stream.of(spans).map(bytesEncoder::encode).collect(toList()));
  }

  byte[] readMessage(String topic) {
    Properties properties = new Properties();
    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.bootstrapServer());
    properties.put(GROUP_ID_CONFIG, "zipkin");
    properties.put(AUTO_OFFSET_RESET_CONFIG, "earliest");

    ByteArrayDeserializer keyDeserializer = new ByteArrayDeserializer();
    ByteArrayDeserializer valueDeserializer = new ByteArrayDeserializer();
    try (Consumer<byte[], byte[]> consumer =
           new KafkaConsumer<>(properties, keyDeserializer, valueDeserializer)) {
      consumer.subscribe(Collections.singletonList(topic));
      return consumer.poll(Duration.ofSeconds(5)).iterator().next().value();
    }
  }

  private byte[] readMessage() {
    return readMessage("zipkin");
  }
}
