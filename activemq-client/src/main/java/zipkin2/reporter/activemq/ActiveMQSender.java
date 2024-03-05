/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin2.reporter.activemq;

import java.io.IOException;
import java.util.List;
import javax.jms.BytesMessage;
import javax.jms.JMSException;
import javax.jms.QueueSender;
import org.apache.activemq.ActiveMQConnectionFactory;
import zipkin2.reporter.AsyncReporter;
import zipkin2.reporter.BytesMessageSender;
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
 * This type is designed for {@link AsyncReporter.Builder#builder(BytesMessageSender) the async
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

    public ActiveMQSender build() {
      if (connectionFactory == null) throw new NullPointerException("connectionFactory == null");
      return new ActiveMQSender(this);
    }

    Builder() {
    }
  }

  final Encoding encoding;
  final int messageMaxBytes;

  final LazyInit lazyInit;

  ActiveMQSender(Builder builder) {
    this.encoding = builder.encoding;
    this.messageMaxBytes = builder.messageMaxBytes;
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

  /** {@inheritDoc} */
  @Override @Deprecated public Call<Void> sendSpans(List<byte[]> encodedSpans) {
    if (closeCalled) throw new ClosedSenderException();
    byte[] message = encoding.encode(encodedSpans);
    return new ActiveMQCall(message);
  }

  /** {@inheritDoc} */
  @Override public void send(List<byte[]> encodedSpans) throws IOException {
    if (closeCalled) throw new ClosedSenderException();
    send(encoding.encode(encodedSpans));
  }

  void send(byte[] message) throws IOException {
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

  /** {@inheritDoc} */
  @Override @Deprecated public CheckResult check() {
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

  @Override public String toString() {
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
      send(message);
      return null;
    }

    @Override public Call<Void> clone() {
      return new ActiveMQCall(message);
    }

    @Override protected void doEnqueue(Callback<Void> callback) {
      try {
        send(message);
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
