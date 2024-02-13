/*
 * Copyright 2016-2024 The OpenZipkin Authors
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
