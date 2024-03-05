/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin2.reporter.libthrift;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import org.apache.thrift.TException;
import zipkin2.reporter.Call;
import zipkin2.reporter.Callback;
import zipkin2.reporter.CheckResult;
import zipkin2.reporter.ClosedSenderException;
import zipkin2.reporter.Encoding;
import zipkin2.reporter.Sender;

/**
 * Blocking reporter that sends spans to Zipkin via Scribe.
 *
 * <p>This sender is not thread-safe.
 */
public final class LibthriftSender extends Sender {
  /** Creates a sender that sends {@link Encoding#THRIFT} messages. */
  public static LibthriftSender create(String host) {
    return newBuilder().host(host).build();
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static final class Builder {
    String host;
    int port = 9410;
    int messageMaxBytes = 500000; // TFramedTransport.DEFAULT_MAX_LENGTH
    int connectTimeout = 10 * 1000, socketTimeout = 60 * 1000;

    Builder(LibthriftSender sender) {
      this.host = sender.host;
      this.messageMaxBytes = sender.messageMaxBytes;
      this.connectTimeout = sender.connectTimeout;
      this.socketTimeout = sender.socketTimeout;
      this.port = sender.port;
    }

    /** No default. The host listening for scribe messages. */
    public Builder host(String host) {
      if (host == null) throw new NullPointerException("host == null");
      this.host = host;
      return this;
    }

    /** Default 9410. The port listening for scribe messages. */
    public Builder port(int port) {
      this.port = port;
      return this;
    }

    /** Default 60 * 1000 milliseconds. 0 implies no timeout. */
    public Builder socketTimeout(int socketTimeout) {
      this.socketTimeout = socketTimeout;
      return this;
    }

    /** Default 10 * 1000 milliseconds. 0 implies no timeout. */
    public Builder connectTimeout(int connectTimeout) {
      this.connectTimeout = connectTimeout;
      return this;
    }

    /** Maximum size of a message. Default 16384000 */
    public Builder messageMaxBytes(int messageMaxBytes) {
      this.messageMaxBytes = messageMaxBytes;
      return this;
    }

    public LibthriftSender build() {
      return new LibthriftSender(this);
    }

    Builder() {
    }
  }

  final String host;
  final int port;
  final int messageMaxBytes;
  final int connectTimeout, socketTimeout;

  LibthriftSender(Builder builder) {
    if (builder.host == null) throw new NullPointerException("host == null");
    this.host = builder.host;
    this.messageMaxBytes = builder.messageMaxBytes;
    this.connectTimeout = builder.connectTimeout;
    this.socketTimeout = builder.socketTimeout;
    this.port = builder.port;
  }

  public Builder toBuilder() {
    return new Builder(this);
  }

  @Override
  public Encoding encoding() {
    return Encoding.THRIFT;
  }

  @Override public int messageMaxBytes() {
    return messageMaxBytes;
  }

  @Override public int messageSizeInBytes(int encodedSizeInBytes) {
    return ScribeClient.messageSizeInBytes(encodedSizeInBytes);
  }

  /** Size of the Thrift RPC message */
  @Override
  public int messageSizeInBytes(List<byte[]> encodedSpans) {
    return ScribeClient.messageSizeInBytes(encodedSpans);
  }

  /** {@inheritDoc} */
  @Override @Deprecated public Call<Void> sendSpans(List<byte[]> encodedSpans) {
    if (closeCalled) throw new ClosedSenderException();
    return new ScribeCall(encodedSpans);
  }

  /** {@inheritDoc} */
  @Override public void send(List<byte[]> encodedSpans) throws IOException {
    if (closeCalled) throw new ClosedSenderException();
    try {
      if (!get().log(encodedSpans)) {
        throw new IllegalStateException("try later");
      }
    } catch (TException e) {
      throw new IOException(e);
    }
  }

  ScribeClient get() {
    if (client == null) {
      synchronized (this) {
        if (client == null) {
          client = new ScribeClient(host, port, socketTimeout, connectTimeout);
        }
      }
    }
    return client;
  }

  /** close is typically called from a different thread */
  private volatile boolean closeCalled;
  private volatile ScribeClient client;

  /** {@inheritDoc} */
  @Override @Deprecated public CheckResult check() {
    try {
      if (get().log(Collections.<byte[]>emptyList())) {
        return CheckResult.OK;
      }
      throw new IllegalStateException("try later");
    } catch (Exception e) {
      return CheckResult.failed(e);
    }
  }

  @Override public void close() {
    if (closeCalled) return;
    closeCalled = true;
    ScribeClient client = this.client;
    if (client != null) client.close();
  }

  @Override public String toString() {
    return "LibthriftSender(" + host + ":" + port + ")";
  }

  class ScribeCall extends Call.Base<Void> {
    final List<byte[]> encodedSpans;

    ScribeCall(List<byte[]> encodedSpans) {
      this.encodedSpans = encodedSpans;
    }

    @Override protected Void doExecute() throws IOException {
      send(encodedSpans);
      return null;
    }

    @Override protected void doEnqueue(Callback<Void> callback) {
      try {
        if (get().log(encodedSpans)) {
          callback.onSuccess(null);
        } else {
          callback.onError(new IllegalStateException("try later"));
        }
        callback.onSuccess(null);
      } catch (Throwable t) {
        Call.propagateIfFatal(t);
        callback.onError(t);
      }
    }

    @Override public Call<Void> clone() {
      return new ScribeCall(encodedSpans);
    }
  }
}
