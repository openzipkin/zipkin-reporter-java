package zipkin2.reporter.kafka;

import java.io.Flushable;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import zipkin2.CheckResult;
import zipkin2.Component;
import zipkin2.Span;
import zipkin2.codec.Encoding;
import zipkin2.codec.SpanBytesEncoder;
import zipkin2.reporter.Reporter;
import zipkin2.reporter.ReporterMetrics;

public class KafkaReporter extends Component implements Reporter<Span>, Flushable {

  /**
   * Configuration including defaults needed to send spans to a Kafka topic.
   */
  public static final class Builder {
    ReporterMetrics metrics = ReporterMetrics.NOOP_METRICS;
    final Properties properties;
    Encoding encoding = Encoding.JSON;
    String topic = "zipkin-span";
    int messageMaxBytes = 500000;
    int batchSize = 100000;
    long lingerMs = 5;

    Builder(Properties properties) {
      this.properties = properties;
    }

    Builder(KafkaReporter reporter) {
      properties = new Properties();
      properties.putAll(reporter.properties);
      encoding = reporter.encoding;
      topic = reporter.topic;
      messageMaxBytes = reporter.messageMaxBytes;
    }

    /**
     * Topic zipkin spans will be send to. Defaults to "zipkin-sender"
     */
    public KafkaReporter.Builder topic(String topic) {
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
    public final KafkaReporter.Builder bootstrapServers(String bootstrapServers) {
      if (bootstrapServers == null) throw new NullPointerException("bootstrapServers == null");
      properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
      return this;
    }

    /**
     * Maximum size of a message. Must be equal to or less than the server's "message.max.bytes" and
     * "replica.fetch.max.bytes" to avoid rejected records on the broker side. Default 500000.
     */
    public KafkaReporter.Builder messageMaxBytes(int messageMaxBytes) {
      this.messageMaxBytes = messageMaxBytes;
      properties.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, messageMaxBytes);
      return this;
    }

    /**
     * Batch size used by producer to group records before sending them to Kafka. Default 100000.
     */
    public KafkaReporter.Builder batchSize(int batchSize) {
      this.batchSize = batchSize;
      properties.put(ProducerConfig.BATCH_SIZE_CONFIG, batchSize);
      return this;
    }

    /**
     * Milliseconds to wait until batch.size is full, else messages grouped are sent. Default 5
     */
    public KafkaReporter.Builder lingerMs(long lingerMs) {
      this.lingerMs = lingerMs;
      properties.put(ProducerConfig.LINGER_MS_CONFIG, lingerMs);
      return this;
    }

    /**
     * By default, a producer will be created, targeted to {@link #bootstrapServers(String)} with 0
     * required {@link ProducerConfig#ACKS_CONFIG acks}. Any properties set here will affect the
     * producer config.
     * <p>
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
    public final KafkaReporter.Builder overrides(Map<String, ?> overrides) {
      if (overrides == null) throw new NullPointerException("overrides == null");
      properties.putAll(overrides);
      return this;
    }

    /**
     * By default, a producer will be created, targeted to {@link #bootstrapServers(String)} with 0
     * required {@link ProducerConfig#ACKS_CONFIG acks}. Any properties set here will affect the
     * producer config.
     * <p>
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
    public final KafkaReporter.Builder overrides(Properties overrides) {
      if (overrides == null) throw new NullPointerException("overrides == null");
      properties.putAll(overrides);
      return this;
    }

    /**
     * Aggregates and reports reporter metrics to a monitoring system. Defaults to no-op.
     */
    public KafkaReporter.Builder metrics(ReporterMetrics metrics) {
      if (metrics == null) throw new NullPointerException("metrics == null");
      this.metrics = metrics;
      return this;
    }

    /**
     * Use this to change the encoding used in messages. Default is {@linkplain Encoding#JSON}
     *
     * <p>Note: If ultimately sending to Zipkin, version 2.8+ is required to process protobuf.
     */
    public KafkaReporter.Builder encoding(Encoding encoding) {
      if (encoding == null) throw new NullPointerException("encoding == null");
      this.encoding = encoding;
      return this;
    }

    public KafkaReporter build() {
      return new KafkaReporter(this);
    }
  }

  final Properties properties;
  final String topic;
  final Encoding encoding;
  final SpanBytesEncoder encoder;
  final int messageMaxBytes;
  final ReporterMetrics metrics;

  public static KafkaReporter create(String bootstrapServers) {
    return newBuilder().bootstrapServers(bootstrapServers).build();
  }

  public static KafkaReporter.Builder newBuilder() {
    // Settings below correspond to "Producer Configs"
    // http://kafka.apache.org/0102/documentation.html#producerconfigs
    Properties properties = new Properties();
    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
      ByteArraySerializer.class.getName());
    // disabling batching as duplicates effort covered by sender buffering.
    properties.put(ProducerConfig.BATCH_SIZE_CONFIG, 0);
    // 1MB, aligned with default kafka max.message.bytes config.
    properties.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, 1000000);
    properties.put(ProducerConfig.ACKS_CONFIG, "0");
    return new KafkaReporter.Builder(properties);
  }

  KafkaReporter(KafkaReporter.Builder builder) {
    properties = new Properties();
    properties.putAll(builder.properties);
    topic = builder.topic;
    encoding = builder.encoding;
    switch (encoding) {
      case JSON:
        encoder = SpanBytesEncoder.JSON_V2;
        break;
      case PROTO3:
        encoder = SpanBytesEncoder.PROTO3;
        break;
      case THRIFT:
        encoder = SpanBytesEncoder.THRIFT;
        break;
      default:
        throw new UnsupportedOperationException(encoding.name());
    }
    messageMaxBytes = builder.messageMaxBytes;
    metrics = builder.metrics;
  }

  public KafkaReporter.Builder toBuilder() {
    return new KafkaReporter.Builder(this);
  }

  /**
   * get and close are typically called from different threads
   */
  volatile KafkaProducer<byte[], byte[]> producer;
  volatile boolean closeCalled;
  volatile AdminClient adminClient;

  @Override public void report(Span span) {
    metrics.incrementSpans(1);
    int spanSizeInBytes = encoder.sizeInBytes(span);
    metrics.incrementSpanBytes(spanSizeInBytes);
    ProducerRecord<byte[], byte[]> record =
      new ProducerRecord<>(topic, span.traceId().getBytes(), encoder.encode(span));
    getProducer().send(record, (metadata, exception) -> {
       if (exception != null) {
         metrics.incrementMessagesDropped(exception);
         metrics.incrementSpansDropped(1);
       }
    });
  }

  /**
   * Ensures there are no problems reading metadata about the topic.
   */
  @Override public CheckResult check() {
    try {
      KafkaFuture<String> maybeClusterId = getAdminClient().describeCluster().clusterId();
      maybeClusterId.get(1, TimeUnit.SECONDS);
      return CheckResult.OK;
    } catch (Exception e) {
      return CheckResult.failed(e);
    }
  }

  KafkaProducer<byte[], byte[]> getProducer() {
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
          adminClient = AdminClient.create(properties);
        }
      }
    }
    return adminClient;
  }

  @Override public void flush() throws IOException {
    getProducer().flush();
  }

  @Override public synchronized void close() {
    if (closeCalled) return;
    KafkaProducer<byte[], byte[]> producer = this.producer;
    if (producer != null) producer.close();
    AdminClient adminClient = this.adminClient;
    if (adminClient != null) adminClient.close(1, TimeUnit.SECONDS);
    closeCalled = true;
  }

  @Override public final String toString() {
    return "KafkaReporter{"
      + "bootstrapServers=" + properties.get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG)
      + ", topic=" + topic
      + "}";
  }
}
