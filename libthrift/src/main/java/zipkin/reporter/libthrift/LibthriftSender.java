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
package zipkin.reporter.libthrift;

import com.google.auto.value.AutoValue;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import zipkin.internal.LazyCloseable;
import zipkin.reporter.Callback;
import zipkin.reporter.Encoding;
import zipkin.reporter.Sender;

/**
 * Blocking reporter that sends spans to Zipkin via Scribe.
 *
 * <p>This sender is not thread-safe.
 */
@AutoValue
public abstract class LibthriftSender extends LazyCloseable<ScribeClient> implements Sender {
  /** Creates a sender that sends {@link Encoding#THRIFT} messages. */
  public static LibthriftSender create(String host) {
    return builder().host(host).build();
  }

  public static Builder builder() {
    return new AutoValue_LibthriftSender.Builder()
        .port(9410)
        .socketTimeout(60 * 1000)
        .connectTimeout(10 * 1000)
        .messageMaxBytes(16384000 /* TFramedTransport.DEFAULT_MAX_LENGTH */);
  }

  @AutoValue.Builder
  public interface Builder {
    /** No default. The host listening for scribe messages. */
    Builder host(String host);

    /** Default 9410. The port listening for scribe messages. */
    Builder port(int port);

    /** Default 60 * 1000 milliseconds. 0 implies no timeout. */
    Builder socketTimeout(int socketTimeout);

    /** Default 10 * 1000 milliseconds. 0 implies no timeout. */
    Builder connectTimeout(int connectTimeout);

    /** Maximum size of a message. Default 16384000 */
    Builder messageMaxBytes(int messageMaxBytes);

    LibthriftSender build();
  }

  public Builder toBuilder() {
    return new AutoValue_LibthriftSender.Builder(this);
  }

  abstract String host();

  abstract int port();

  abstract int socketTimeout();

  abstract int connectTimeout();

  @Override public Encoding encoding() {
    return Encoding.THRIFT;
  }

  /** Size of the Thrift RPC message */
  @Override public int messageSizeInBytes(List<byte[]> encodedSpans) {
    return ScribeClient.messageSizeInBytes(encodedSpans);
  }

  @Override protected ScribeClient compute() {
    return new ScribeClient(host(), port(), socketTimeout(), connectTimeout());
  }

  /** close is typically called from a different thread */
  private transient boolean closeCalled;

  @Override public void sendSpans(List<byte[]> encodedSpans, Callback callback) {
    if (closeCalled) throw new IllegalStateException("closed");
    try {
      if (get().log(encodedSpans)) {
        callback.onComplete();
      } else {
        callback.onError(new IllegalStateException("try later"));
      }
    } catch (Throwable e) {
      callback.onError(e);
      if (e instanceof Error) throw (Error) e;
    }
  }

  /** Sends an empty log message to the configured host. */
  @Override public CheckResult check() {
    try {
      if (get().log(Collections.emptyList())) {
        return CheckResult.OK;
      }
      throw new IllegalStateException("try later");
    } catch (Exception e) {
      return CheckResult.failed(e);
    }
  }

  @Override public void close() throws IOException {
    if (closeCalled) return;
    closeCalled = true;
    super.close();
  }

  LibthriftSender() {
  }
}
