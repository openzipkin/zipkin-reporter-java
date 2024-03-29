/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin2.reporter.internal;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import zipkin2.reporter.BytesMessageSender;
import zipkin2.reporter.Call;
import zipkin2.reporter.Callback;
import zipkin2.reporter.CheckResult;
import zipkin2.reporter.Encoding;
import zipkin2.reporter.Sender;

/**
 * Reduces burden on types that need to extend {@linkplain Sender}.
 */
public abstract class SenderAdapter extends Sender {
  protected abstract BytesMessageSender delegate();

  @Override public final int messageSizeInBytes(List<byte[]> encodedSpans) {
    return delegate().messageSizeInBytes(encodedSpans);
  }

  @Override public final int messageSizeInBytes(int encodedSizeInBytes) {
    return delegate().messageSizeInBytes(encodedSizeInBytes);
  }

  @Override public final Encoding encoding() {
    return delegate().encoding();
  }

  @Override public final int messageMaxBytes() {
    return delegate().messageMaxBytes();
  }

  @Override @Deprecated public final Call<Void> sendSpans(List<byte[]> encodedSpans) {
    return new SendSpans(encodedSpans);
  }

  @Override public final void send(List<byte[]> encodedSpans) throws IOException {
    delegate().send(encodedSpans);
  }

  @Override @Deprecated public final CheckResult check() {
    try {
      delegate().send(Collections.<byte[]>emptyList());
      return CheckResult.OK;
    } catch (Throwable e) {
      Call.propagateIfFatal(e);
      return CheckResult.failed(e);
    }
  }

  @Override public final void close() {
    try {
      delegate().close();
    } catch (IOException e) {
      throw Platform.get().uncheckedIOException(e);
    }
  }

  @Override public final String toString() {
    return delegate().toString();
  }

  final class SendSpans extends Call.Base<Void> {
    private final List<byte[]> encodedSpans;

    SendSpans(List<byte[]> encodedSpans) {
      this.encodedSpans = encodedSpans;
    }

    @Override protected Void doExecute() throws IOException {
      send(encodedSpans);
      return null;
    }

    @Override protected void doEnqueue(Callback<Void> callback) {
      try {
        send(encodedSpans);
        callback.onSuccess(null);
      } catch (Throwable t) {
        Call.propagateIfFatal(t);
        callback.onError(t);
      }
    }

    @Override public Call<Void> clone() {
      return new SendSpans(encodedSpans);
    }
  }
}
