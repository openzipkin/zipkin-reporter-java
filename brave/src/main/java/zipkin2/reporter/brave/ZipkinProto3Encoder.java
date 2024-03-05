/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin2.reporter.brave;

import brave.Tag;
import brave.Tags;
import brave.handler.MutableSpan;
import zipkin2.reporter.BytesEncoder;
import zipkin2.reporter.Encoding;
import zipkin2.reporter.brave.internal.ZipkinProto3Writer;

final class ZipkinProto3Encoder implements BytesEncoder<MutableSpan> {
  static final BytesEncoder<MutableSpan> INSTANCE = new ZipkinProto3Encoder(Tags.ERROR);
  final ZipkinProto3Writer delegate;

  ZipkinProto3Encoder(Tag<Throwable> errorTag) {
    if (errorTag == null) throw new NullPointerException("errorTag == null");
    this.delegate = new ZipkinProto3Writer(errorTag);
  }

  @Override public Encoding encoding() {
    return Encoding.PROTO3;
  }

  @Override public int sizeInBytes(MutableSpan span) {
    return delegate.sizeInBytes(span);
  }

  @Override public byte[] encode(MutableSpan span) {
    return delegate.write(span);
  }
}
