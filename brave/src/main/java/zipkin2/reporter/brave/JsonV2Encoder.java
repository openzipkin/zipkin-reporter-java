/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin2.reporter.brave;

import brave.Tag;
import brave.Tags;
import brave.handler.MutableSpan;
import brave.handler.MutableSpanBytesEncoder;
import zipkin2.reporter.BytesEncoder;
import zipkin2.reporter.Encoding;

final class JsonV2Encoder implements BytesEncoder<MutableSpan> {
  static final BytesEncoder<MutableSpan> INSTANCE = new JsonV2Encoder(Tags.ERROR);
  final MutableSpanBytesEncoder delegate;

  JsonV2Encoder(Tag<Throwable> errorTag) {
    this.delegate = MutableSpanBytesEncoder.zipkinJsonV2(errorTag);
  }

  @Override public Encoding encoding() {
    return Encoding.JSON;
  }

  @Override public int sizeInBytes(MutableSpan span) {
    return delegate.sizeInBytes(span);
  }

  @Override public byte[] encode(MutableSpan span) {
    return delegate.encode(span);
  }
}
