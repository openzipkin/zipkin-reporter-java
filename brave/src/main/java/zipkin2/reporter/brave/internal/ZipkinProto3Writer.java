/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin2.reporter.brave.internal;

import brave.Tag;
import brave.handler.MutableSpan;

import static zipkin2.reporter.brave.internal.Proto3Fields.sizeOfLengthDelimitedField;

/**
 * Stripped version of {@linkplain zipkin2.internal.Proto3SpanWriter}, which only can write a single
 * span at a time.
 */
public final class ZipkinProto3Writer {

  final ZipkinProto3Fields.SpanField spanField;

  public ZipkinProto3Writer(Tag<Throwable> errorTag) {
    this.spanField = new ZipkinProto3Fields.SpanField(errorTag);
  }

  public int sizeInBytes(MutableSpan span) {
    return spanField.sizeInBytes(span);
  }

  @Override public String toString() {
    return "MutableSpan";
  }

  public byte[] write(MutableSpan span) {
    int sizeOfSpan = spanField.sizeOfValue(span);
    byte[] result = new byte[sizeOfLengthDelimitedField(sizeOfSpan)];
    WriteBuffer buf = new WriteBuffer(result);
    buf.writeByte(spanField.key);
    buf.writeVarint(sizeOfSpan); // length prefix
    spanField.writeValue(buf, span);
    return result;
  }
}
