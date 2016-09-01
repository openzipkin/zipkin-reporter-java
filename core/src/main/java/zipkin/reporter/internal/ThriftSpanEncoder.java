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
package zipkin.reporter.internal;

import zipkin.Codec;
import zipkin.Span;
import zipkin.reporter.Encoder;
import zipkin.reporter.Encoding;

public final class ThriftSpanEncoder implements Encoder<Span> {

  @Override public Encoding encoding() {
    return Encoding.THRIFT;
  }

  @Override public byte[] encode(Span span) {
    return Codec.THRIFT.writeSpan(span);
  }
}
