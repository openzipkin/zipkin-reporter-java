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
package zipkin2.reporter.activemq;

import java.io.IOException;
import java.util.List;
import javax.jms.BytesMessage;
import javax.jms.JMSException;
import javax.jms.QueueSender;
import org.apache.activemq.ActiveMQConnectionFactory;
import zipkin2.reporter.BytesMessageEncoder;
import zipkin2.reporter.Call;
import zipkin2.reporter.Callback;
import zipkin2.reporter.CheckResult;
import zipkin2.reporter.ClosedSenderException;
import zipkin2.reporter.Encoding;
import zipkin2.reporter.Sender;

/**
 * This sends (usually json v2) encoded spans to an ActiveMQ queue.
 *
 * <h3>Usage</h3>
 * <p>
 * This type is designed for {@link zipkin2.reporter.AsyncReporter.Builder#builder(Sender) the async
 * reporter}.
 *
 * <p>Here's a simple configuration, configured for json:
 *
 * <pre>{@code
 * sender = ActiveMQSender.create("failover:tcp://localhost:61616");
 * }</pre>
 *
 * <p>Here's an example with an explicit connection factory and protocol buffers encoding:
 *
 * <pre>{@code
 * connectionFactory = new ActiveMQConnectionFactory(username, password, brokerUrl);
 * connectionFactory.setClientIDPrefix("zipkin");
 * connectionFactory.setConnectionIDPrefix("zipkin");
 * sender = ActiveMQSender.newBuilder()
 *   .connectionFactory(connectionFactory)
 *   .encoding(Encoding.PROTO3)
 *   .build();
 * }</pre>
 *
 * <h3>Compatibility with Zipkin Server</h3>
 *
 * <a href="https://github.com/openzipkin/zipkin">Zipkin server</a> should be v2.15 or higher.
 *
 * <h3>Implementation Notes</h3>
 *
 * <p>This sender is thread-safe.
 */
public final class ActiveMQSender extends Sender {

  public static ActiveMQSender create(String brokerUrl) {
    ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory();
    connectionFactory.setBrokerURL(brokerUrl);
    return newBuilder().connectionFactory(connectionFactory).build();
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static final class Builder {
    ActiveMQConnectionFactory connectionFactory;
    String queue = "zipkin";
    Encoding encoding = Encoding.JSON;
    int messageMaxBytes = 500000;

    public Builder connectionFactory(ActiveMQConnectionFactory connectionFactory) {
      if (connectionFactory == null) throw new NullPointerException("connectionFactory == null");
      this.connectionFactory = connectionFactory;
      return this;
    }

    /** Queue zipkin spans will be consumed from. Defaults to "zipkin". */
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

    /** Maximum size of a message. Default 500KB. */
    public Builder messageMaxBytes(int messageMaxBytes) {
      this.messageMaxBytes = messageMaxBytes;
      return this;
    }

    public final ActiveMQSender build() {
      if (connectionFactory == null) throw new NullPointerException("connectionFactory == null");
      return new ActiveMQSender(this);
    }

    Builder() {
    }
  }

  final Encoding encoding;
  final int messageMaxBytes;
  final BytesMessageEncoder encoder;

  final LazyInit lazyInit;

  ActiveMQSender(Builder builder) {
    this.encoding = builder.encoding;
    this.messageMaxBytes = builder.messageMaxBytes;
    this.encoder = BytesMessageEncoder.forEncoding(encoding);
    this.lazyInit = new LazyInit(builder);
  }

  /** get and close are typically called from different threads */
  volatile boolean closeCalled;

  @Override public Encoding encoding() {
    return encoding;
  }

  @Override public int messageMaxBytes() {
    return messageMaxBytes;
  }

  @Override public int messageSizeInBytes(int encodedSizeInBytes) {
    return encoding.listSizeInBytes(encodedSizeInBytes);
  }

  @Override public int messageSizeInBytes(List<byte[]> encodedSpans) {
    return encoding.listSizeInBytes(encodedSpans);
  }

  @Override public Call<Void> sendSpans(List<byte[]> encodedSpans) {
    if (closeCalled) throw new ClosedSenderException();
    byte[] message = encoder.encode(encodedSpans);
    return new ActiveMQCall(message);
  }

  @Override public CheckResult check() {
    try {
      lazyInit.get();
    } catch (Throwable t) {
      Call.propagateIfFatal(t);
      return CheckResult.failed(t);
    }
    return lazyInit.result.checkResult;
  }

  @Override public void close() {
    closeCalled = true;
    lazyInit.close();
  }

  @Override public final String toString() {
    return "ActiveMQSender{"
      + "brokerURL=" + lazyInit.connectionFactory.getBrokerURL()
      + ", queue=" + lazyInit.queue
      + "}";
  }

  final class ActiveMQCall extends Call.Base<Void> { // ActiveMQCall is not cancelable
    final byte[] message;

    ActiveMQCall(byte[] message) {
      this.message = message;
    }

    @Override protected Void doExecute() throws IOException {
      send();
      return null;
    }

    void send() throws IOException {
      try {
        ActiveMQConn conn = lazyInit.get();
        QueueSender sender = conn.sender;
        BytesMessage bytesMessage = conn.session.createBytesMessage();
        bytesMessage.writeBytes(message);
        sender.send(bytesMessage);
      } catch (JMSException e) {
        throw ioException("Unable to send message: ", e);
      }
    }

    @Override public Call<Void> clone() {
      return new ActiveMQCall(message);
    }

    @Override protected void doEnqueue(Callback<Void> callback) {
      try {
        send();
        callback.onSuccess(null);
      } catch (Throwable t) {
        Call.propagateIfFatal(t);
        callback.onError(t);
      }
    }
  }

  static IOException ioException(String prefix, JMSException e) {
    Exception cause = e.getLinkedException();
    if (cause instanceof IOException) {
      return new IOException(prefix + message(cause), cause);
    }
    return new IOException(prefix + message(e), e);
  }

  static String message(Exception cause) {
    return cause.getMessage() != null ? cause.getMessage() : cause.getClass().getSimpleName();
  }
}
