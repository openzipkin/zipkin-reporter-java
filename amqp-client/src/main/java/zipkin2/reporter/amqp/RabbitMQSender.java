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
package zipkin2.reporter.amqp;

import com.rabbitmq.client.Address;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeoutException;
import zipkin2.reporter.BytesMessageEncoder;
import zipkin2.reporter.Call;
import zipkin2.reporter.Callback;
import zipkin2.reporter.CheckResult;
import zipkin2.reporter.ClosedSenderException;
import zipkin2.reporter.Encoding;
import zipkin2.reporter.Sender;

import static zipkin2.reporter.Call.propagateIfFatal;

/**
 * This sends (usually json v2) encoded spans to a RabbitMQ queue.
 *
 * <h3>Usage</h3>
 * <p>
 * This type is designed for {@link zipkin2.reporter.AsyncReporter.Builder#builder(Sender) the async
 * reporter}.
 *
 * <p>Here's a simple configuration, configured for json:
 *
 * <pre>{@code
 * sender = RabbitMQSender.create("localhost:5672");
 * }</pre>
 *
 * <p>Here's an example with an explicit SSL connection factory and protocol buffers encoding:
 *
 * <pre>{@code
 * connectionFactory = new ConnectionFactory();
 * connectionFactory.setHost("localhost");
 * connectionFactory.setPort(5671);
 * connectionFactory.useSslProtocol();
 * sender = RabbitMQSender.newBuilder()
 *   .connectionFactory(connectionFactory)
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
 * <p>The sender does not use <a href="https://www.rabbitmq.com/confirms.html">RabbitMQ Publisher
 * Confirms</a>, so messages considered sent may not necessarily be received by consumers in case of
 * RabbitMQ failure.
 *
 * <p>This sender is thread-safe: a channel is created for each thread that calls
 * {@link #sendSpans(List)}.
 */
public final class RabbitMQSender extends Sender {
  /** Creates a sender that sends {@link Encoding#JSON} messages. */
  public static RabbitMQSender create(String addresses) {
    return newBuilder().addresses(addresses).build();
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  /** Configuration including defaults needed to send spans to a RabbitMQ queue. */
  public static final class Builder {
    ConnectionFactory connectionFactory = new ConnectionFactory();
    List<Address> addresses;
    String queue = "zipkin";
    Encoding encoding = Encoding.JSON;
    int messageMaxBytes = 500000;

    Builder(RabbitMQSender sender) {
      connectionFactory = sender.connectionFactory.clone();
      addresses = sender.addresses;
      queue = sender.queue;
      encoding = sender.encoding;
      messageMaxBytes = sender.messageMaxBytes;
    }

    public Builder connectionFactory(ConnectionFactory connectionFactory) {
      if (connectionFactory == null) throw new NullPointerException("connectionFactory == null");
      this.connectionFactory = connectionFactory;
      return this;
    }

    public Builder addresses(List<Address> addresses) {
      if (addresses == null) throw new NullPointerException("addresses == null");
      this.addresses = addresses;
      return this;
    }

    /** Comma-separated list of host:port pairs. ex "192.168.99.100:5672" No Default. */
    public Builder addresses(String addresses) {
      if (addresses == null) throw new NullPointerException("addresses == null");
      this.addresses = convertAddresses(addresses);
      return this;
    }

    /** Queue zipkin spans will be send to. Defaults to "zipkin" */
    public Builder queue(String queue) {
      if (queue == null) throw new NullPointerException("queue == null");
      this.queue = queue;
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

    /** Connection TCP establishment timeout in milliseconds. Defaults to 60 seconds */
    public Builder connectionTimeout(int connectionTimeout) {
      connectionFactory.setConnectionTimeout(connectionTimeout);
      return this;
    }

    /** The virtual host to use when connecting to the broker. Defaults to "/" */
    public Builder virtualHost(String virtualHost) {
      connectionFactory.setVirtualHost(virtualHost);
      return this;
    }

    /** The AMQP user name to use when connecting to the broker. Defaults to "guest" */
    public Builder username(String username) {
      connectionFactory.setUsername(username);
      return this;
    }

    /** The password to use when connecting to the broker. Defaults to "guest" */
    public Builder password(String password) {
      connectionFactory.setPassword(password);
      return this;
    }

    /** Maximum size of a message. Default 500KB. */
    public Builder messageMaxBytes(int messageMaxBytes) {
      this.messageMaxBytes = messageMaxBytes;
      return this;
    }

    public final RabbitMQSender build() {
      return new RabbitMQSender(this);
    }

    Builder() {
    }
  }

  final Encoding encoding;
  final int messageMaxBytes;
  final List<Address> addresses;
  final String queue;
  final ConnectionFactory connectionFactory;
  final BytesMessageEncoder encoder;

  RabbitMQSender(Builder builder) {
    if (builder.addresses == null) throw new NullPointerException("addresses == null");
    encoding = builder.encoding;
    encoder = BytesMessageEncoder.forEncoding(encoding);
    messageMaxBytes = builder.messageMaxBytes;
    addresses = builder.addresses;
    queue = builder.queue;
    connectionFactory = builder.connectionFactory.clone();
  }

  public Builder toBuilder() {
    return new Builder(this);
  }

  /** get and close are typically called from different threads */
  volatile Connection connection;
  volatile boolean closeCalled;

  @Override public Encoding encoding() {
    return encoding;
  }

  @Override public int messageMaxBytes() {
    return messageMaxBytes;
  }

  @Override public int messageSizeInBytes(List<byte[]> encodedSpans) {
    return encoding.listSizeInBytes(encodedSpans);
  }

  @Override public int messageSizeInBytes(int encodedSizeInBytes) {
    return encoding.listSizeInBytes(encodedSizeInBytes);
  }

  /** This sends all of the spans as a single message. */
  @Override public Call<Void> sendSpans(List<byte[]> encodedSpans) {
    if (closeCalled) throw new ClosedSenderException();
    byte[] message = encoder.encode(encodedSpans);
    return new RabbitMQCall(message);
  }

  /** Ensures there are no connection issues. */
  @Override public CheckResult check() {
    try {
      if (localChannel().isOpen()) return CheckResult.OK;
      throw new IllegalStateException("Not Open");
    } catch (Throwable e) {
      propagateIfFatal(e);
      return CheckResult.failed(e);
    }
  }

  @Override public String toString() {
    return "RabbitMQSender{addresses=" + addresses + ", queue=" + queue + "}";
  }

  Connection get() {
    if (connection == null) {
      synchronized (this) {
        if (connection == null) {
          connection = newConnection();
        }
      }
    }
    return connection;
  }

  Connection newConnection() {
    try {
      return connectionFactory.newConnection(addresses);
    } catch (IOException e) {
      throw new RuntimeException("Unable to establish connection to RabbitMQ server", e);
    } catch (TimeoutException e) {
      throw new RuntimeException("Unable to establish connection to RabbitMQ server", e);
    }
  }

  @Override public synchronized void close() throws IOException {
    if (closeCalled) return;
    Connection connection = this.connection;
    if (connection != null) connection.close();
    closeCalled = true;
  }

  final ThreadLocal<Channel> CHANNEL = new ThreadLocal<Channel>();

  /**
   * In most circumstances there will only be one thread calling {@link #sendSpans(List)}, the
   * {@link AsyncReporter}. Just in case someone is flushing manually, we use a thread-local. All of
   * this is to avoid recreating a channel for each publish, as that costs two additional network
   * roundtrips.
   */
  Channel localChannel() throws IOException {
    Channel channel = CHANNEL.get();
    if (channel == null) {
      channel = get().createChannel();
      CHANNEL.set(channel);
    }
    return channel;
  }

  class RabbitMQCall extends Call.Base<Void> { // RabbitMQFuture is not cancelable
    private final byte[] message;

    RabbitMQCall(byte[] message) {
      this.message = message;
    }

    @Override protected Void doExecute() throws IOException {
      publish();
      return null;
    }

    void publish() throws IOException {
      localChannel().basicPublish("", queue, null, message);
    }

    @Override protected void doEnqueue(Callback<Void> callback) {
      try {
        publish();
        callback.onSuccess(null);
      } catch (Throwable t) {
        Call.propagateIfFatal(t);
        callback.onError(t);
      }
    }

    @Override public Call<Void> clone() {
      return new RabbitMQCall(message);
    }
  }

  static List<Address> convertAddresses(String addresses) {
    String[] addressStrings = addresses.split(",");
    Address[] addressArray = new Address[addressStrings.length];
    for (int i = 0; i < addressStrings.length; i++) {
      String[] splitAddress = addressStrings[i].split(":");
      String host = splitAddress[0];
      Integer port = null;
      try {
        if (splitAddress.length == 2) port = Integer.parseInt(splitAddress[1]);
      } catch (NumberFormatException ignore) {
      }
      addressArray[i] = (port != null) ? new Address(host, port) : new Address(host);
    }
    return Arrays.asList(addressArray);
  }
}
