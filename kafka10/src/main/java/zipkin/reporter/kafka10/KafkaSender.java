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

import com.google.auto.value.AutoValue;
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
import zipkin.reporter.BytesMessageEncoder;
import zipkin.reporter.Callback;
import zipkin.reporter.Encoding;
import zipkin.reporter.Sender;

import static zipkin.internal.Util.checkNotNull;

/**
 * This sends (usually TBinaryProtocol big-endian) encoded spans to a Kafka topic.
 *
 * <p>This sender is thread-safe.
 *
 * <p>This sender is linked against Kafka 0.10.2+, which allows it to work with Kafka 0.10+ brokers
 */
@AutoValue
public abstract class KafkaSender extends LazyCloseable<KafkaProducer<byte[], byte[]>>
    implements Sender {

  public static KafkaSender create(String bootstrapServers) {
    return builder().bootstrapServers(bootstrapServers).build();
  }

  public static Builder builder() {
    // Settings below correspond to "Producer Configs"
    // http://kafka.apache.org/0102/documentation.html#producerconfigs
    Properties properties = new Properties();
    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        ByteArraySerializer.class.getName());
    properties.put(ProducerConfig.ACKS_CONFIG, "0");
    return new AutoValue_KafkaSender.Builder()
        .encoding(Encoding.THRIFT)
        .properties(properties)
        .topic("zipkin")
        .overrides(Collections.EMPTY_MAP)
        .messageMaxBytes(1000000);
  }

  /** Configuration including defaults needed to send spans to a Kafka topic. */
  @AutoValue.Builder
  public static abstract class Builder {
    abstract Builder properties(Properties properties);

    /** Topic zipkin spans will be send to. Defaults to "zipkin" */
    public abstract Builder topic(String topic);

    abstract Properties properties();

    /**
     * Initial set of kafka servers to connect to, rest of cluster will be discovered (comma
     * separated). No default
     *
     * @see ProducerConfig#BOOTSTRAP_SERVERS_CONFIG
     */
    public final Builder bootstrapServers(String bootstrapServers) {
      properties().put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
          checkNotNull(bootstrapServers, "bootstrapServers"));
      return this;
    }

    /**
     * Maximum size of a message. Must be equal to or less than the server's "message.max.bytes".
     * Default 1000000.
     */
    public abstract Builder messageMaxBytes(int messageMaxBytes);

    /**
     * By default, a producer will be created, targeted to {@link #bootstrapServers(String)} with 0
     * required {@link ProducerConfig#ACKS_CONFIG acks}. Any properties set here will affect the
     * producer config.
     *
     * <p>For example: Reduce the timeout blocking from one minute to 5 seconds.
     * <pre>{@code
     * Map<String, String> overrides = new LinkedHashMap<>();
     * overrides.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "5000");
     * builder.overrides(overrides);
     * }</pre>
     *
     * @see ProducerConfig
     */
    public final Builder overrides(Map<String, String> overrides) {
      properties().putAll(checkNotNull(overrides, "overrides"));
      return this;
    }

    public abstract Builder encoding(Encoding encoding);

    abstract Encoding encoding();

    public final KafkaSender build() {
      return encoder(BytesMessageEncoder.forEncoding(encoding())).autoBuild();
    }

    abstract Builder encoder(BytesMessageEncoder encoder);

    public abstract KafkaSender autoBuild();

    Builder() {
    }
  }

  public abstract Builder toBuilder();

  abstract BytesMessageEncoder encoder();

  abstract String topic();

  abstract Properties properties();

  /** close is typically called from a different thread */
  volatile boolean closeCalled;

  @Override public int messageSizeInBytes(List<byte[]> encodedSpans) {
    return encoding().listSizeInBytes(encodedSpans);
  }

  /**
   * This sends all of the spans as a single message.
   *
   * <p>NOTE: this blocks until the metadata server is available.
   */
  @SuppressWarnings("FutureReturnValueIgnored") // callbacks are used instead
  @Override public void sendSpans(List encodedSpans, Callback callback) {
    if (closeCalled) throw new IllegalStateException("closed");
    try {
      final byte[] message = encoder().encode(encodedSpans);
      get().send(new ProducerRecord<>(topic(), message), (metadata, exception) -> {
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

  /** Ensures there are no problems reading metadata about the topic. */
  @Override public CheckResult check() {
    try {
      get().partitionsFor(topic()); // make sure we can query the metadata
      return CheckResult.OK;
    } catch (RuntimeException e) {
      return CheckResult.failed(e);
    }
  }

  @Override protected KafkaProducer<byte[], byte[]> compute() {
    return new KafkaProducer<>(properties());
  }

  @Override public void close() throws IOException {
    if (closeCalled) return;
    closeCalled = true;
    super.close();
  }

  @Override public final String toString() {
    return "KafkaSender(" + properties().getProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG) + ")";
  }

  KafkaSender() {
  }
}
