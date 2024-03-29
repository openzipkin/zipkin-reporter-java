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

/** Includes built-in formats used in Zipkin. */
public enum MutableSpanBytesEncoder implements BytesEncoder<MutableSpan> {
  /** Corresponds to the Zipkin v2 json format */
  JSON_V2 {
    @Override public Encoding encoding() {
      return Encoding.JSON;
    }

    @Override public int sizeInBytes(MutableSpan input) {
      return JsonV2Encoder.INSTANCE.sizeInBytes(input);
    }

    @Override public byte[] encode(MutableSpan input) {
      return JsonV2Encoder.INSTANCE.encode(input);
    }
  },
  PROTO3 {
    @Override public Encoding encoding() {
      return Encoding.PROTO3;
    }

    @Override public int sizeInBytes(MutableSpan input) {
      return ZipkinProto3Encoder.INSTANCE.sizeInBytes(input);
    }

    @Override public byte[] encode(MutableSpan input) {
      return ZipkinProto3Encoder.INSTANCE.encode(input);
    }
  };

  /**
   * Returns the default {@linkplain MutableSpan} encoder for given encoding.
   *
   * @throws UnsupportedOperationException if the encoding is not yet supported.
   * @since 3.3
   */
  public static BytesEncoder<MutableSpan> forEncoding(Encoding encoding) {
    if (encoding == null) throw new NullPointerException("encoding == null");
    switch (encoding) {
      case JSON:
        return JSON_V2;
      case PROTO3:
        return PROTO3;
      case THRIFT:
        throw new UnsupportedOperationException("THRIFT is not yet a built-in encoder");
      default: // BUG: as encoding is an enum!
        throw new UnsupportedOperationException("BUG: " + encoding.name());
    }
  }

  /**
   * Like {@linkplain #forEncoding(Encoding)}, except you can override the default throwable parser,
   * which is {@linkplain brave.Tags#ERROR}.
   *
   * @since 3.3
   */
  public static BytesEncoder<MutableSpan> create(Encoding encoding, Tag<Throwable> errorTag) {
    if (encoding == null) throw new NullPointerException("encoding == null");
    if (errorTag == null) throw new NullPointerException("errorTag == null");
    if (errorTag == Tags.ERROR) return forEncoding(encoding);
    switch (encoding) {
      case JSON:
        return new JsonV2Encoder(errorTag);
      case PROTO3:
        return new ZipkinProto3Encoder(errorTag);
      case THRIFT:
        throw new UnsupportedOperationException("THRIFT is not yet a built-in encoder");
      default: // BUG: as encoding is an enum!
        throw new UnsupportedOperationException("BUG: " + encoding.name());
    }
  }
}
