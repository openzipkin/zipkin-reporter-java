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
