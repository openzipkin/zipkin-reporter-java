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
package zipkin.reporter.local;

import com.google.auto.value.AutoValue;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import zipkin.Codec;
import zipkin.Span;
import zipkin.internal.Nullable;
import zipkin.reporter.Callback;
import zipkin.reporter.Encoding;
import zipkin.reporter.Sender;
import zipkin.storage.AsyncSpanConsumer;
import zipkin.storage.StorageComponent;

/**
 * Sender that decodes spans and offers them to a {@link StorageComponent}
 *
 * <p>This sender is not thread-safe.
 */
@AutoValue
public abstract class LocalSender implements Sender {
  /** Creates a sender that sends {@link Encoding#THRIFT} messages. */
  public static LocalSender create(StorageComponent storage) {
    return builder().storage(storage).build();
  }

  public static Builder builder() {
    return new AutoValue_LocalSender.Builder()
        .messageMaxBytes(5 * 1024 * 1024 /* arbitrary */);
  }

  @AutoValue.Builder
  public interface Builder {
    /** No default. The storage component to send spans to. */
    Builder storage(StorageComponent storage);

    /**
     * Maximum size of spans to {@link AsyncSpanConsumer#accept(List, zipkin.storage.Callback)
     * accept} at a time. Default 5MiB
     */
    Builder messageMaxBytes(int messageMaxBytes);

    LocalSender build();
  }

  public Builder toBuilder() {
    return new AutoValue_LocalSender.Builder(this);
  }

  abstract StorageComponent storage();

  @Override public Encoding encoding() {
    return Encoding.THRIFT;
  }

  @Override public int messageSizeInBytes(List<byte[]> encodedSpans) {
    return Encoding.THRIFT.listSizeInBytes(encodedSpans);
  }

  /** close is typically called from a different thread */
  private volatile boolean closeCalled;

  @Override public void sendSpans(List<byte[]> encodedSpans, Callback callback) {
    if (closeCalled) throw new IllegalStateException("closed");
    try {
      List<Span> spans = new ArrayList<>(encodedSpans.size());
      for (byte[] encodedSpan : encodedSpans) {
        spans.add(Codec.THRIFT.readSpan(encodedSpan));
      }
      storage().asyncSpanConsumer().accept(spans, new CallbackAdapter(callback));
    } catch (Throwable e) {
      callback.onError(e);
      if (e instanceof Error) throw (Error) e;
    }
  }

  /** Delegates to the local storage's check operation. */
  @Override public CheckResult check() {
    return storage().check();
  }

  @Override public void close() throws IOException {
    if (closeCalled) return;
    closeCalled = true;
    // not closing the storage component as we didn't create it
  }

  LocalSender() {
  }

  static final class CallbackAdapter implements zipkin.storage.Callback<Void> {
    final Callback delegate;

    public CallbackAdapter(Callback delegate) {
      this.delegate = delegate;
    }

    @Override public void onSuccess(@Nullable Void aVoid) {
      delegate.onComplete();
    }

    @Override public void onError(Throwable throwable) {
      delegate.onError(throwable);
    }
  }
}
