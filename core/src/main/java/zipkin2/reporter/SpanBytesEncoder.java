/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin2.reporter;

import zipkin2.Span;

/** Includes built-in formats used in Zipkin. */
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

  /**
   * Returns the default {@linkplain Span} encoder for given encoding.
   *
   * @since 3.3
   */
  public static BytesEncoder<Span> forEncoding(Encoding encoding) {
    if (encoding == null) throw new NullPointerException("encoding == null");
    switch (encoding) {
      case JSON:
        return JSON_V2;
      case PROTO3:
        return PROTO3;
      case THRIFT:
        return THRIFT;
      default: // BUG: as encoding is an enum!
        throw new UnsupportedOperationException("BUG: " + encoding.name());
    }
  }
}
