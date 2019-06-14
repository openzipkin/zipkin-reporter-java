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
package zipkin2.reporter.libthrift;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import org.apache.thrift.TException;
import zipkin2.Call;
import zipkin2.Callback;
import zipkin2.CheckResult;
import zipkin2.codec.Encoding;
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
    int messageMaxBytes = 16384000; // TFramedTransport.DEFAULT_MAX_LENGTH
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

    public final LibthriftSender build() {
      return new LibthriftSender(this);
    }

    Builder() {}
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

  @Override public Call<Void> sendSpans(List<byte[]> encodedSpans) {
    if (closeCalled) throw new IllegalStateException("closed");
    return new ScribeCall(encodedSpans);
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

  /** Sends an empty log message to the configured host. */
  @Override
  public CheckResult check() {
    try {
      if (get().log(Collections.emptyList())) {
        return CheckResult.OK;
      }
      throw new IllegalStateException("try later");
    } catch (Exception e) {
      return CheckResult.failed(e);
    }
  }

  @Override
  public void close() throws IOException {
    if (closeCalled) return;
    closeCalled = true;
    super.close();
  }

  @Override
  public final String toString() {
    return "LibthriftSender(" + host + ":" + port + ")";
  }

  class ScribeCall extends Call.Base<Void> {
    final List<byte[]> encodedSpans;

    ScribeCall(List<byte[]> encodedSpans) {
      this.encodedSpans = encodedSpans;
    }

    @Override protected Void doExecute() throws IOException {
      try {
        if (!get().log(encodedSpans)) {
          throw new IllegalStateException("try later");
        }
      } catch (TException e) {
        throw new IOException(e);
      }
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
      } catch (TException |RuntimeException | Error e) {
        callback.onError(e);
      }
    }

    @Override public Call<Void> clone() {
      return new ScribeCall(encodedSpans);
    }
  }
}
