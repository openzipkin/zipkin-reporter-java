/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin2.reporter.pulsar;

import io.opentelemetry.api.internal.StringUtils;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import zipkin2.reporter.AsyncReporter;
import zipkin2.reporter.BytesMessageSender;
import zipkin2.reporter.Call;
import zipkin2.reporter.Callback;
import zipkin2.reporter.CheckResult;
import zipkin2.reporter.ClosedSenderException;
import zipkin2.reporter.Encoding;
import zipkin2.reporter.Sender;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * This sends (usually json v2) encoded spans to a Pulsar topic.
 *
 * <h3>Usage</h3>
 * <p>
 * This type is designed for {@link AsyncReporter.Builder#builder(BytesMessageSender) the async
 * reporter}.
 *
 * <p>Here's a simple configuration, configured for json:
 *
 * <pre>{@code
 * sender = PulsarSender.create("pulsar://localhost:6650");
 * }</pre>
 *
 * <p>Here's to customize the required configuration for pulsar's
 * client, produce, and message, or override the default configuration:
 *
 * <pre>{@code
 * Map<String, Object> clientPropsMap = new HashMap<>();
 * clientPropsMap.put("numIoThreads", 20);
 * Map<String, Object> producerPropsMap = new HashMap<>();
 * producerPropsMap.put("producerName", "zipkin");
 * Map<String, Object> messagePropsMap = new HashMap<>();
 * messagePropsMap.put("key", "zipkin-key");
 * sender = PulsarSender.newBuilder()
 *   .serviceUrl("pulsar://localhost:6650")
 *   .topic("zipkin")
 *   .clientProps(clientPropsMap)
 *   .producerProps(producerPropsMap)
 *   .messageProps(messagePropsMap)
 *   .encoding(Encoding.PROTO3)
 *   .build();
 * }</pre>
 *
 * <h3>Compatibility with Zipkin Server</h3>
 *
 * <a href="https://github.com/openzipkin/zipkin">Zipkin server</a> should be v2.1 or higher.
 *
 * <h3>Implementation Notes</h3>
 *
 * <p>This sender is thread-safe.<p>
 * <p>clientProps {@link org.apache.pulsar.client.impl.conf.ClientConfigurationData}<p>
 * <p>producerProps {@link org.apache.pulsar.client.impl.conf.ProducerConfigurationData}<p>
 * <p>messageProps {@link org.apache.pulsar.client.api.TypedMessageBuilder}<p>
 *
 * @since 3.5
 */
public final class PulsarSender extends Sender {
  /** Creates a sender that sends {@link Encoding#JSON} messages. */
  public static PulsarSender create(String serviceUrl) {
    return newBuilder().serviceUrl(serviceUrl).build();
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static final class Builder {
    Map<String, Object> clientProps = new HashMap<>(),
      producerProps = new HashMap<>(), messageProps = new HashMap<>();
    String topic = "zipkin";
    Encoding encoding = Encoding.JSON;
    int messageMaxBytes = 500_000;

    public PulsarSender build() {
      return new PulsarSender(this);
    }

    Builder() {
    }

    /** The service URL for the Pulsar client ex. pulsar://my-broker:6650. No default. */
    public Builder serviceUrl(String serviceUrl) {
      if (StringUtils.isNullOrEmpty(serviceUrl)) throw new NullPointerException("serviceUrl is null or empty");
      clientProps.put("serviceUrl", serviceUrl);
      return this;
    }

    /** Specify the topic this producer will be publishing on. Defaults to "zipkin" */
    public Builder topic(String topic) {
      if (StringUtils.isNullOrEmpty(topic)) throw new NullPointerException("topic is null or empty");
      this.topic = topic;
      return this;
    }

    /**
     * Maximum size of a message. Must be equal to or less than the broker's "maxMessageSize" to
     * avoid rejected records on the broker side. Default 500KB.
     */
    public Builder messageMaxBytes(int messageMaxBytes) {
      this.messageMaxBytes = messageMaxBytes;
      producerProps.put("batchingMaxBytes", messageMaxBytes);
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

    /**
     * Load the configuration from provided <tt>config</tt> map.
     * Any properties set here will override the previous Pulsar client configuration.
     *
     * <p>For example:
     * <pre>{@code
     * Map<String, Object> clientPropsMap = new HashMap<>();
     * clientPropsMap.put("numIoThreads", 20);
     * builder.clientProps(clientPropsMap);
     * }</pre>
     *
     * @param clientPropsMap Map<String, Object>
     * @return Builder
     * @see org.apache.pulsar.client.impl.conf.ClientConfigurationData
     */
    public Builder clientProps(Map<String, Object> clientPropsMap) {
      if (clientPropsMap.isEmpty()) throw new NullPointerException("clientProps is empty");
      clientProps.putAll(clientPropsMap);
      return this;
    }

    /**
     * By default, a producer will be created, targeted to {@link #serviceUrl(String)}
     * Any properties set here will override the previous Pulsar producer configuration.
     * <p>
     * Consider not overriding batching properties `enableBatching=false` as it will
     * duplicate buffering effort that is already handled by Sender.
     *
     * <p>For example: Config the producerName.
     * <pre>{@code
     * Map<String, Object> producerPropsMap = new HashMap<>();
     * producerPropsMap.put("producerName", "zipkin");
     * builder.producerProps(producerPropsMap);
     * }</pre>
     *
     * @param producerPropsMap Map<String, Object>
     * @return Builder
     * @see org.apache.pulsar.client.impl.conf.ProducerConfigurationData
     */
    public Builder producerProps(Map<String, Object> producerPropsMap) {
      if (producerPropsMap.isEmpty()) throw new NullPointerException("producerProps is empty");
      producerProps.putAll(producerPropsMap);
      return this;
    }

    /**
     * Configure messages.
     * Any properties set here will override the previous Pulsar message configuration.
     *
     * <p>For example: Set various properties of Pulsar's messages.
     * <pre>{@code
     * Map<String, Object> messagePropsMap = new HashMap<>();
     * messagePropsMap.put("key", "zipkin-key");
     * messagePropsMap.put("eventTime", System.currentTimeMillis());
     * builder.messageProps(messagePropsMap);
     * }</pre>
     *
     * @param messagePropsMap Map<String, Object>
     * @return Builder
     * @see org.apache.pulsar.client.impl.TypedMessageBuilderImpl#loadConf
     */
    public Builder messageProps(Map<String, Object> messagePropsMap) {
      if (messagePropsMap.isEmpty()) throw new NullPointerException("messageProps is empty");
      messageProps.putAll(messagePropsMap);
      return this;
    }
  }

  final Map<String, Object> clientProps, producerProps, messageProps;
  final String topic;
  final Encoding encoding;
  final int messageMaxBytes;

  PulsarSender(Builder builder) {
    clientProps = builder.clientProps;
    producerProps = builder.producerProps;
    messageProps = builder.messageProps;
    topic = builder.topic;
    encoding = builder.encoding;
    messageMaxBytes = builder.messageMaxBytes;
  }

  volatile boolean closeCalled;
  volatile Producer<byte[]> producer;
  volatile PulsarClient client;

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

  /** {@inheritDoc} */
  @Override @Deprecated public Call<Void> sendSpans(List<byte[]> encodedSpans) {
    if (closeCalled) throw new ClosedSenderException();
    byte[] message = encoding.encode(encodedSpans);
    return new PulsarCall(message);
  }

  @Override public void send(List<byte[]> encodedSpans) {
    if (closeCalled) throw new ClosedSenderException();
    sender(encoding.encode(encodedSpans));
  }

  void sender(byte[] message) {
    if (closeCalled) throw new ClosedSenderException();
    sendMessage(message);
  }

  void sendMessage(byte[] message) {
    if (client == null) {
      synchronized (this) {
        if (client == null) {
          client = createClient();
          producer = createProducer(client);

          try {
            producer.newMessage()
              .value(message)
              .loadConf(messageProps)
              .sendAsync();
          } catch (Exception e) {
            cleanup();
            throw new RuntimeException("Pulsar producer send failed." + e.getMessage(), e);
          }
        }
      }
    }
  }

  Producer<byte[]> createProducer(PulsarClient client) {
    try {
      producer = client.newProducer()
        .topic(topic)
        .enableBatching(false) // disabling batching as duplicates effort covered by sender buffering.
        .loadConf(producerProps)
        .create();
    } catch (Exception e) {
      cleanup();
      throw new RuntimeException("Pulsar producer creation failed." + e.getMessage(), e);
    }
    return producer;
  }

  PulsarClient createClient() {
    try {
      client = PulsarClient.builder()
        .loadConf(clientProps)
        .build();
    } catch (PulsarClientException e) {
      throw new RuntimeException("Pulsar client creation failed. " + e.getMessage(), e);
    }
    return client;
  }

  void cleanup() {
    try {
      if (producer != null) {
        producer.close();
        producer = null;
      }
    } catch (PulsarClientException ignored) {
    }
    try {
      if (client != null) {
        client.close();
        client = null;
      }
    } catch (PulsarClientException ignored) {
    }
  }

  /** {@inheritDoc} */
  @Override @Deprecated public CheckResult check() {
    try {
      client.getPartitionsForTopic(topic).get(15, TimeUnit.SECONDS);
    } catch (Throwable t) {
      Call.propagateIfFatal(t);
      return CheckResult.failed(t);
    }
    return CheckResult.OK;
  }

  @Override public synchronized void close() throws IOException {
    if (closeCalled) return;
    PulsarClient client = this.client;
    if (client != null) client.close();
    Producer<byte[]> producer = this.producer;
    if (producer != null) producer.close();
    closeCalled = true;
  }

  @Override public String toString() {
    return "PulsarSender{" +
      "clientProps=" + clientProps +
      ", producerProps=" + producerProps +
      ", messageProps=" + messageProps +
      ", topic=" + topic +
      '}';
  }

  class PulsarCall extends Call.Base<Void> {  // PulsarCall is not cancelable
    private final byte[] message;

    PulsarCall(byte[] message) {
      this.message = message;
    }

    @Override protected Void doExecute() throws IOException {
      sender(message);
      return null;
    }

    @Override protected void doEnqueue(Callback<Void> callback) {
      try {
        sender(message);
        callback.onSuccess(null);
      } catch (Throwable t) {
        Call.propagateIfFatal(t);
        callback.onError(t);
      }
    }

    @Override public Call<Void> clone() {
      return new PulsarCall(message);
    }
  }
}
