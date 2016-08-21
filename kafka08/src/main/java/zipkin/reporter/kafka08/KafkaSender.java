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

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import zipkin.internal.LazyCloseable;
import zipkin.internal.Nullable;
import zipkin.reporter.Callback;
import zipkin.reporter.Encoding;
import zipkin.reporter.ListEncoder;
import zipkin.reporter.Sender;
import zipkin.reporter.SpanEncoder;

import static zipkin.internal.Util.checkNotNull;

/**
 * This sends (usually TBinaryProtocol big-endian) encoded spans to a Kafka topic.
 *
 * <p>This sender remains a Kafka 0.8.x consumer, while Zipkin systems update to 0.9+.
 */
public final class KafkaSender implements Sender {

  public static Builder builder() {
    return new Builder();
  }

  /** Configuration including defaults needed to send spans to a Kafka topic. */
  public static final class Builder {
    String topic = "zipkin";
    String bootstrapServers;
    Map<String, String> overrides = Collections.emptyMap();
    ListEncoder listEncoder;

    /** Topic zipkin spans will be send to. Defaults to "zipkin" */
    public Builder topic(String topic) {
      this.topic = checkNotNull(topic, "topic");
      return this;
    }

    /**
     * Initial set of kafka servers to connect to, rest of cluster will be discovered (comma
     * separated). No default
     *
     * @see ProducerConfig#BOOTSTRAP_SERVERS_CONFIG
     */
    public Builder bootstrapServers(String bootstrapServers) {
      this.bootstrapServers = checkNotNull(bootstrapServers, "bootstrapServers");
      return this;
    }

    /**
     * By default, a producer will be created, targeted to {@link #bootstrapServers(String)} with 0
     * required {@link ProducerConfig#ACKS_CONFIG acks}. Any properties set here will affect the
     * producer config.
     *
     * <p>For example: Reduce the timeout blocking on metadata from one minute to 5 seconds.
     * <pre>{@code
     * Map<String, String> overrides = new LinkedHashMap<>();
     * overrides.put(ProducerConfig.METADATA_FETCH_TIMEOUT_CONFIG, "5000");
     * builder.overrides(overrides);
     * }</pre>
     *
     * @see ProducerConfig
     */
    public Builder overrides(Map<String, String> overrides) {
      this.overrides = checkNotNull(overrides, "overrides");
      return this;
    }

    /**
     * Controls how to encode a list of spans into a single message. Defaults to detect based on the
     * first span sent.
     *
     * <p>For example, if you are using {@link SpanEncoder#JSON} or similar, use {@link
     * ListEncoder#JSON}. If you are using {@link SpanEncoder#THRIFT} or similar, use {@link
     * ListEncoder#THRIFT}
     */
    public Builder listEncoder(ListEncoder listEncoder) {
      this.listEncoder = listEncoder;
      return this;
    }

    public KafkaSender build() {
      return new KafkaSender(this);
    }

    Builder() {
    }
  }

  final LazyCloseable<KafkaProducer<byte[], byte[]>> producer;
  final String topic;
  final @Nullable ListEncoder listEncoder;

  KafkaSender(Builder builder) {
    producer = new LazyProducer(builder);
    topic = builder.topic;
    listEncoder = builder.listEncoder;
  }

  /**
   * This sends all of the spans as a single message.
   *
   * <p>NOTE: this blocks until the metadata server is available.
   */
  @Override public void sendSpans(List<byte[]> spans, Callback callback) {
    try {
      final byte[] message = encodeMessage(spans);
      producer.get().send(new ProducerRecord<>(topic, message), (metadata, exception) -> {
        if (exception == null) {
          callback.onComplete();
        } else {
          callback.onError(exception);
        }
      });
    } catch (Throwable e) {
      callback.onError(e);
      if (e instanceof Error) throw (Error) e;
    }
  }

  byte[] encodeMessage(List<byte[]> spans) {
    final byte[] message;
    if (listEncoder != null) {
      message = listEncoder.encode(spans);
    } else {
      Encoding encoding = Encoding.detectFromSpan(spans.get(0));
      switch (encoding) {
        case JSON:
          message = ListEncoder.JSON.encode(spans);
          break;
        case THRIFT:
          message = ListEncoder.THRIFT.encode(spans);
          break;
        default:
          throw new UnsupportedOperationException("Unsupported encoding: " + encoding.name());
      }
    }
    return message;
  }

  /** Ensures there are no problems reading metadata about the topic. */
  @Override public CheckResult check() {
    try {
      producer.get().partitionsFor(topic); // make sure we can query the metadata
      return CheckResult.OK;
    } catch (RuntimeException e) {
      return CheckResult.failed(e);
    }
  }

  @Override
  public void close() throws IOException {
    producer.close();
  }

  static final class LazyProducer extends LazyCloseable<KafkaProducer<byte[], byte[]>> {

    final Properties props;

    LazyProducer(Builder builder) {
      // Settings below correspond to "Producer Configs"
      // http://kafka.apache.org/08/documentation.html#producerconfigs
      Properties props = new Properties();
      props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, builder.bootstrapServers);
      props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
      props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
      props.put(ProducerConfig.ACKS_CONFIG, "0");
      props.putAll(builder.overrides);
      this.props = props;
    }

    @Override protected KafkaProducer<byte[], byte[]> compute() {
      return new KafkaProducer<>(props);
    }

    @Override
    public void close() {
      KafkaProducer<byte[], byte[]> maybeNull = maybeNull();
      if (maybeNull != null) maybeNull.close();
    }
  }
}
