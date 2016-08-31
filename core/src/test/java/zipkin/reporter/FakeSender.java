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
package zipkin.reporter;

import com.google.auto.value.AutoValue;
import java.io.IOException;
import java.util.List;
import java.util.function.Consumer;
import zipkin.Codec;
import zipkin.Span;

@AutoValue
public abstract class FakeSender implements Sender<byte[]> {
  static FakeSender create() {
    return new AutoValue_FakeSender(
        MessageEncoder.THRIFT_BYTES,
        Integer.MAX_VALUE,
        Encoding.THRIFT,
        Codec.THRIFT,
        spans -> {
        }
    );
  }

  FakeSender onSpans(Consumer<List<Span>> onSpans) {
    return new AutoValue_FakeSender(
        encoder(),
        messageMaxBytes(),
        spanEncoding(),
        codec(),
        onSpans
    );
  }

  FakeSender json() {
    return new AutoValue_FakeSender(
        MessageEncoder.JSON_BYTES,
        messageMaxBytes(),
        Encoding.JSON,
        Codec.JSON,
        onSpans()
    );
  }

  FakeSender messageMaxBytes(int messageMaxBytes) {
    return new AutoValue_FakeSender(
        encoder(),
        messageMaxBytes,
        spanEncoding(),
        codec(),
        onSpans()
    );
  }

  abstract Codec codec();

  abstract Consumer<List<Span>> onSpans();

  @Override public void sendSpans(List<byte[]> encodedSpans, Callback callback) {
    try {
      onSpans().accept(codec().readSpans(encoder().encode(encodedSpans)));
      callback.onComplete();
    } catch (Throwable t) {
      callback.onError(t);
    }
  }

  @Override public CheckResult check() {
    return CheckResult.OK;
  }

  @Override public void close() throws IOException {
  }

  @Override public String toString() {
    return "FakeSender";
  }

  FakeSender() {
  }
}
