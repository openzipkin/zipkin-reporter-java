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
package zipkin2.reporter;

import zipkin2.Span;

/** Includes built-in formats used in Zipkin. */
@SuppressWarnings("ImmutableEnumChecker") // because span is immutable
public enum SpanBytesEncoder implements BytesEncoder<Span> {
  /** Corresponds to the Zipkin v1 thrift format */
  THRIFT {
    @Override public Encoding encoding() {
      return Encoding.THRIFT;
    }

    @Override public int sizeInBytes(Span input) {
      return zipkin2.codec.SpanBytesEncoder.THRIFT.sizeInBytes(input);
    }

    @Override public byte[] encode(Span input) {
      return zipkin2.codec.SpanBytesEncoder.THRIFT.encode(input);
    }
  },
  /** Corresponds to the Zipkin v1 json format */
  JSON_V1 {
    @Override public Encoding encoding() {
      return Encoding.JSON;
    }

    @Override public int sizeInBytes(Span input) {
      return zipkin2.codec.SpanBytesEncoder.JSON_V1.sizeInBytes(input);
    }

    @Override public byte[] encode(Span input) {
      return zipkin2.codec.SpanBytesEncoder.JSON_V1.encode(input);
    }
  },
  /** Corresponds to the Zipkin v2 json format */
  JSON_V2 {
    @Override public Encoding encoding() {
      return Encoding.JSON;
    }

    @Override public int sizeInBytes(Span input) {
      return zipkin2.codec.SpanBytesEncoder.JSON_V2.sizeInBytes(input);
    }

    @Override public byte[] encode(Span input) {
      return zipkin2.codec.SpanBytesEncoder.JSON_V2.encode(input);
    }
  },
  PROTO3 {
    @Override public Encoding encoding() {
      return Encoding.PROTO3;
    }

    @Override public int sizeInBytes(Span input) {
      return zipkin2.codec.SpanBytesEncoder.PROTO3.sizeInBytes(input);
    }

    @Override public byte[] encode(Span input) {
      return zipkin2.codec.SpanBytesEncoder.PROTO3.encode(input);
    }
  };
}
