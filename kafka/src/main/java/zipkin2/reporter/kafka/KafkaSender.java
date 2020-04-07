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

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import zipkin2.Call;
import zipkin2.Callback;
import zipkin2.CheckResult;
import zipkin2.codec.Encoding;
import zipkin2.reporter.AsyncReporter;
import zipkin2.reporter.AwaitableCallback;
import zipkin2.reporter.BytesMessageEncoder;
import zipkin2.reporter.ClosedSenderException;
import zipkin2.reporter.Sender;

/**
 * This sends (usually json v2) encoded spans to a Kafka topic.
 *
 * <h3>Usage</h3>
 *
 * This type is designed for {@link AsyncReporter.Builder#builder(Sender) the async reporter}.
 *
 * <p>Here's a simple configuration, configured for json:
 *
 * <pre>{@code
 * sender = KafkaSender.create("localhost:9092");
 * }</pre>
 *
 * <p>Here's an example that overrides properties and protocol buffers encoding:
 *
 * <pre>{@code
 * Properties overrides = new Properties();
 * overrides.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 5000);
 * sender = KafkaSender.newBuilder()
 *   .bootstrapServers("host1:9092,host2:9092")
 *   .overrides(overrides)
 *   .encoding(Encoding.PROTO3)
 *   .build();
 * }</pre>
 *
 * <h3>Compatibility with Zipkin Server</h3>
 *
 * <a href="https://github.com/openzipkin/zipkin">Zipkin server</a> should be v1.26 or higher.
 *
 * <h3>Implementation Notes</h3>
 *
 * <p>This sender is thread-safe. This sender is linked against Kafka 0.10.2+, which allows it to
 * work with Kafka 0.10+ brokers
 */
public final class KafkaSender extends Sender {
  /** Creates a sender that sends {@link Encoding#JSON} messages. */
  public static KafkaSender create(String bootstrapServers) {
    return newBuilder().bootstrapServers(bootstrapServers).build();
  }

  public static Builder newBuilder() {
    // Settings below correspond to "Producer Configs"
    // http://kafka.apache.org/0102/documentation.html#producerconfigs
    Properties properties = new Properties();
    Properties adminClientProperties = new Properties();
    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
      ByteArraySerializer.class.getName());
    // disabling batching as duplicates effort covered by sender buffering.
    properties.put(ProducerConfig.BATCH_SIZE_CONFIG, 0);
    properties.put(ProducerConfig.ACKS_CONFIG, "0");
    return new Builder(properties);
  }

  /** Configuration including defaults needed to send spans to a Kafka topic. */
  public static final class Builder {
    final Properties properties;
    Properties adminClientProperties;
    Encoding encoding = Encoding.JSON;
    String topic = "zipkin";
    int messageMaxBytes = 500_000;

    Builder(Properties properties) {
      this.properties = properties;
      this.adminClientProperties = new Properties();
    }

    Builder(KafkaSender sender) {
      properties = new Properties();
      properties.putAll(sender.properties);
      adminClientProperties = new Properties();
      encoding = sender.encoding;
      topic = sender.topic;
      messageMaxBytes = sender.messageMaxBytes;
    }

    /** Topic zipkin spans will be send to. Defaults to "zipkin" */
    public Builder topic(String topic) {
      if (topic == null) throw new NullPointerException("topic == null");
      this.topic = topic;
      return this;
    }

    /**
     * Initial set of kafka servers to connect to, rest of cluster will be discovered (comma
     * separated). Ex "192.168.99.100:9092" No default
     *
     * @see ProducerConfig#BOOTSTRAP_SERVERS_CONFIG
     */
    public final Builder bootstrapServers(String bootstrapServers) {
      if (bootstrapServers == null) throw new NullPointerException("bootstrapServers == null");
      properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
      return this;
    }

    /**
     * Maximum size of a message. Must be equal to or less than the server's "message.max.bytes" and
     * "replica.fetch.max.bytes" to avoid rejected records on the broker side.
     * Default 500KB.
     */
    public Builder messageMaxBytes(int messageMaxBytes) {
      this.messageMaxBytes = messageMaxBytes;
      properties.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, messageMaxBytes);
      return this;
    }

    /**
     * By default, a producer will be created, targeted to {@link #bootstrapServers(String)} with 0
     * required {@link ProducerConfig#ACKS_CONFIG acks}. Any properties set here will affect the
     * producer config.
     *
     * Consider not overriding batching properties ("batch.size" and "linger.ms") as those will
     * duplicate buffering effort that is already handled by Sender.
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
    public final Builder overrides(Map<String, ?> overrides) {
      if (overrides == null) throw new NullPointerException("overrides == null");
      properties.putAll(overrides);
      return this;
    }

    /**
     * Any properties set here will affect the admin client config.
     *
     * Consider not overriding batching properties ("batch.size" and "linger.ms") as those will
     * duplicate buffering effort that is already handled by Sender.
     *
     * <p>For example: Configure de number of retries to 5.
     * <pre>{@code
     * Properties overridesAdminClient = new Properties();
     * overridesAdminClient.put(AdminClientConfig.RETRIES_CONFIG, 5);
     * builder.overridesAdminClient(overridesAdminClient);
     * }</pre>
     *
     * @see AdminClientConfig
     */
    public final Builder overridesAdminClient(Map<String, ?> overrides) {
      if (overrides == null) throw new NullPointerException("overrides == null");
      adminClientProperties.putAll(overrides);
      return this;
    }

    /**
     * By default, a producer will be created, targeted to {@link #bootstrapServers(String)} with 0
     * required {@link ProducerConfig#ACKS_CONFIG acks}. Any properties set here will affect the
     * producer config.
     *
     * Consider not overriding batching properties ("batch.size" and "linger.ms") as those will
     * duplicate buffering effort that is already handled by Sender.
     *
     * <p>For example: Reduce the timeout blocking from one minute to 5 seconds.
     * <pre>{@code
     * Properties overrides = new Properties();
     * overrides.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 5000);
     * builder.overrides(overrides);
     * }</pre>
     *
     * @see ProducerConfig
     */
    public final Builder overrides(Properties overrides) {
      if (overrides == null) throw new NullPointerException("overrides == null");
      properties.putAll(overrides);
      return this;
    }

    /**
     * Any properties set here will affect the admin client config.
     *
     * Consider not overriding batching properties ("batch.size" and "linger.ms") as those will
     * duplicate buffering effort that is already handled by Sender.
     *
     * <p>For example: Configure de number of retries to 5.
     * <pre>{@code
     * Properties overridesAdminClient = new Properties();
     * overridesAdminClient.put(AdminClientConfig.RETRIES_CONFIG, 5);
     * builder.overridesAdminClient(overridesAdminClient);
     * }</pre>
     *
     * @see AdminClientConfig
     */
    public final Builder overridesAdminClient(Properties overrides) {
      if (overrides == null) throw new NullPointerException("overrides == null");
      adminClientProperties.putAll(overrides);
      return this;
    }

    /**
     * Use this to change the encoding used in messages. Default is {@linkplain Encoding#JSON}
     *
     * <p>Note: If ultimately sending to Zipkin, version 2.8+ is required to process protobuf.
     */
    public Builder encoding(Encoding encoding) {
      if (encoding == null) throw new NullPointerException("encoding == null");
      this.encoding = encoding;
      return this;
    }

    public KafkaSender build() {
      return new KafkaSender(this);
    }
  }

  final Properties properties;
  final Properties adminClientProperties;
  final String topic;
  final Encoding encoding;
  final BytesMessageEncoder encoder;
  final int messageMaxBytes;

  KafkaSender(Builder builder) {
    properties = new Properties();
    adminClientProperties = new Properties();
    properties.putAll(builder.properties);
    adminClientProperties.putAll(filterPropertiesForAdminClient(properties));
    adminClientProperties.putAll(builder.adminClientProperties);
    topic = builder.topic;
    encoding = builder.encoding;
    encoder = BytesMessageEncoder.forEncoding(builder.encoding);
    messageMaxBytes = builder.messageMaxBytes;
  }

  /**
   * Filter the properties configured for the producer by removing those not used for the Admin Client.
   *
   * See @{@link AdminClientConfig} config properties
   */
  private Map<String, Object> filterPropertiesForAdminClient(Properties properties){
    Map<String, Object> mapResult = new HashMap<>();
    for (Entry property: properties.entrySet()) {
      if (AdminClientConfig.configNames().contains(property.getKey())){
        mapResult.put(property.getKey().toString(),property.getValue());
      }
    }
    return mapResult;
  }

  public Builder toBuilder() {
    return new Builder(this);
  }

  /** get and close are typically called from different threads */
  volatile KafkaProducer<byte[], byte[]> producer;
  volatile boolean closeCalled;
  volatile AdminClient adminClient;

  @Override public int messageSizeInBytes(List<byte[]> encodedSpans) {
    return encoding.listSizeInBytes(encodedSpans);
  }

  @Override public int messageSizeInBytes(int encodedSizeInBytes) {
    return encoding.listSizeInBytes(encodedSizeInBytes);
  }

  @Override public Encoding encoding() {
    return encoding;
  }

  @Override public int messageMaxBytes() {
    return messageMaxBytes;
  }

  /**
   * This sends all of the spans as a single message.
   *
   * <p>NOTE: this blocks until the metadata server is available.
   */
  @Override public zipkin2.Call<Void> sendSpans(List<byte[]> encodedSpans) {
    if (closeCalled) throw new ClosedSenderException();
    byte[] message = encoder.encode(encodedSpans);
    return new KafkaCall(message);
  }

  /** Ensures there are no problems reading metadata about the topic. */
  @Override public CheckResult check() {
    try {
      KafkaFuture<String> maybeClusterId = getAdminClient().describeCluster().clusterId();
      maybeClusterId.get(1, TimeUnit.SECONDS);
      return CheckResult.OK;
    } catch (Exception e) {
      return CheckResult.failed(e);
    }
  }

  KafkaProducer<byte[], byte[]> get() {
    if (producer == null) {
      synchronized (this) {
        if (producer == null) {
          producer = new KafkaProducer<>(properties);
        }
      }
    }
    return producer;
  }


  AdminClient getAdminClient() {
    if (adminClient == null) {
      synchronized (this) {
        if (adminClient == null) {
          adminClient = AdminClient.create(adminClientProperties);
        }
      }
    }
    return adminClient;
  }

  @Override public synchronized void close() {
    if (closeCalled) return;
    KafkaProducer<byte[], byte[]>  producer = this.producer;
    if (producer != null) producer.close();
    AdminClient adminClient = this.adminClient;
    if (adminClient != null) adminClient.close(1, TimeUnit.SECONDS);
    closeCalled = true;
  }

  @Override public final String toString() {
    return "KafkaSender{"
      + "bootstrapServers=" + properties.get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG)
      + ", topic=" + topic
      + "}";
  }

  class KafkaCall extends Call.Base<Void> { // KafkaFuture is not cancelable
    private final byte[] message;

    KafkaCall(byte[] message) {
      this.message = message;
    }

    @Override protected Void doExecute() throws IOException {
      AwaitableCallback callback = new AwaitableCallback();
      get().send(new ProducerRecord<>(topic, message), new CallbackAdapter(callback));
      callback.await();
      return null;
    }

    @Override protected void doEnqueue(Callback<Void> callback) {
      get().send(new ProducerRecord<>(topic, message), new CallbackAdapter(callback));
    }

    @Override public Call<Void> clone() {
      return new KafkaCall(message);
    }
  }

  static final class CallbackAdapter implements org.apache.kafka.clients.producer.Callback {
    final Callback<Void> delegate;

    CallbackAdapter(Callback<Void> delegate) {
      this.delegate = delegate;
    }

    @Override public void onCompletion(RecordMetadata metadata, Exception exception) {
      if (exception == null) {
        delegate.onSuccess(null);
      } else {
        delegate.onError(exception);
      }
    }

    @Override public String toString() {
      return delegate.toString();
    }
  }
}
