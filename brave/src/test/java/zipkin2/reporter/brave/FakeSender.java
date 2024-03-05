/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin2.reporter.brave;

import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import zipkin2.Span;
import zipkin2.codec.BytesDecoder;
import zipkin2.codec.SpanBytesDecoder;
import zipkin2.reporter.BytesEncoder;
import zipkin2.reporter.BytesMessageSender;
import zipkin2.reporter.ClosedSenderException;
import zipkin2.reporter.Encoding;
import zipkin2.reporter.SpanBytesEncoder;

public final class FakeSender extends BytesMessageSender.Base {

  public static FakeSender create() {
    return new FakeSender(Encoding.JSON, Integer.MAX_VALUE, SpanBytesEncoder.JSON_V2,
      SpanBytesDecoder.JSON_V2, spans -> {
    });
  }

  final int messageMaxBytes;
  final BytesEncoder<Span> encoder;
  final BytesDecoder<Span> decoder;
  final Consumer<List<Span>> onSpans;

  FakeSender(Encoding encoding, int messageMaxBytes, BytesEncoder<Span> encoder,
    BytesDecoder<Span> decoder, Consumer<List<Span>> onSpans) {
    super(encoding);
    this.messageMaxBytes = messageMaxBytes;
    this.encoder = encoder;
    this.decoder = decoder;
    this.onSpans = onSpans;
  }

  FakeSender encoding(Encoding encoding) {
    return new FakeSender(encoding, messageMaxBytes, // invalid but not needed, yet
      encoder, // invalid but not needed, yet
      decoder, // invalid but not needed, yet
      onSpans);
  }

  FakeSender onSpans(Consumer<List<Span>> onSpans) {
    return new FakeSender(encoding, messageMaxBytes, encoder, decoder, onSpans);
  }

  @Override public int messageMaxBytes() {
    return messageMaxBytes;
  }

  /** close is typically called from a different thread */
  volatile boolean closeCalled;

  @Override public void send(List<byte[]> encodedSpans) {
    if (closeCalled) throw new ClosedSenderException();
    List<Span> decoded = encodedSpans.stream().map(decoder::decodeOne).collect(Collectors.toList());
    onSpans.accept(decoded);
  }

  @Override public void close() {
    closeCalled = true;
  }

  @Override public String toString() {
    return "FakeSender";
  }
}
