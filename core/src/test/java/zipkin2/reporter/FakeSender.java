/*
 * Copyright 2016-2018 The OpenZipkin Authors
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
package zipkin2.reporter;

import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import zipkin2.Call;
import zipkin2.CheckResult;
import zipkin2.Span;
import zipkin2.codec.BytesDecoder;
import zipkin2.codec.BytesEncoder;
import zipkin2.codec.Encoding;
import zipkin2.codec.SpanBytesDecoder;
import zipkin2.codec.SpanBytesEncoder;

public final class FakeSender extends Sender {
  static FakeSender create() {
    return new FakeSender(
        Encoding.JSON,
        Integer.MAX_VALUE,
        BytesMessageEncoder.forEncoding(Encoding.JSON),
        SpanBytesEncoder.JSON_V2,
        SpanBytesDecoder.JSON_V2,
        spans -> {
        }
    );
  }

  final Encoding encoding;
  final int messageMaxBytes;
  final BytesMessageEncoder messageEncoder;
  final BytesEncoder<Span> encoder;
  final BytesDecoder<Span> decoder;
  final Consumer<List<Span>> onSpans;

  FakeSender(
      Encoding encoding,
      int messageMaxBytes,
      BytesMessageEncoder messageEncoder,
      BytesEncoder<Span> encoder,
      BytesDecoder<Span> decoder,
      Consumer<List<Span>> onSpans
  ) {
    this.encoding = encoding;
    this.messageMaxBytes = messageMaxBytes;
    this.messageEncoder = messageEncoder;
    this.encoder = encoder;
    this.decoder = decoder;
    this.onSpans = onSpans;
  }

  FakeSender encoding(Encoding encoding) {
    return new FakeSender(
        encoding,
        messageMaxBytes,
        messageEncoder, // invalid but not needed, yet
        encoder, // invalid but not needed, yet
        decoder, // invalid but not needed, yet
        onSpans
    );
  }

  FakeSender onSpans(Consumer<List<Span>> onSpans) {
    return new FakeSender(
        encoding,
        messageMaxBytes,
        messageEncoder,
        encoder,
        decoder,
        onSpans
    );
  }

  FakeSender messageMaxBytes(int messageMaxBytes) {
    return new FakeSender(
        encoding,
        messageMaxBytes,
        messageEncoder,
        encoder,
        decoder,
        onSpans
    );
  }

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

  /** close is typically called from a different thread */
  volatile boolean closeCalled;

  @Override public Call<Void> sendSpans(List<byte[]> encodedSpans) {
    if (closeCalled) throw new IllegalStateException("closed");
    List<Span> decoded = encodedSpans.stream()
        .map(s -> decoder.decodeOne(s))
        .collect(Collectors.toList());
    onSpans.accept(decoded);
    return Call.create(null);
  }

  @Override public CheckResult check() {
    return CheckResult.OK;
  }

  @Override public void close() {
    closeCalled = true;
  }

  @Override public String toString() {
    return "FakeSender";
  }
}
