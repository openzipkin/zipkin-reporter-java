/*
 * Copyright 2016-2020 The OpenZipkin Authors
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
import brave.handler.MutableSpan;
import brave.handler.MutableSpanBytesEncoder;
import java.util.List;
import zipkin2.codec.BytesEncoder;
import zipkin2.codec.Encoding;

final class JsonV2Encoder implements BytesEncoder<MutableSpan> {
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

  @Override public byte[] encodeList(List<MutableSpan> spans) {
    return delegate.encodeList(spans);
  }
}
