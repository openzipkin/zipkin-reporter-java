/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin2.reporter;

import java.util.List;

/**
 * Senders like Kafka use byte[] message encoding. This provides helpers to concatenate spans into a
 * list.
 *
 * @deprecated As of 3.3, use {@linkplain Encoding#encode(List)}. This will be removed in v4.0.
 */
@Deprecated
public enum BytesMessageEncoder {
  JSON {
    @Override public byte[] encode(List<byte[]> values) {
      int sizeOfArray = 2;
      int length = values.size();
      for (int i = 0; i < length; ) {
        sizeOfArray += values.get(i++).length;
        if (i < length) sizeOfArray++;
      }

      byte[] buf = new byte[sizeOfArray];
      int pos = 0;
      buf[pos++] = '[';
      for (int i = 0; i < length; ) {
        byte[] v = values.get(i++);
        System.arraycopy(v, 0, buf, pos, v.length);
        pos += v.length;
        if (i < length) buf[pos++] = ',';
      }
      buf[pos] = ']';
      return buf;
    }
  },
  /**
   * The first format of Zipkin was TBinaryProtocol, big-endian thrift. It is no longer used, but
   * defined here to allow legacy code to migrate to the current reporter library.
   *
   * <p>This writes the list header followed by a concatenation of the input.
   *
   * @see Encoding#THRIFT
   * @deprecated this format is deprecated in favor of json or proto3
   */
  @Deprecated
  THRIFT {
    @Override public byte[] encode(List<byte[]> values) {
      int sizeOfArray = 5;
      int length = values.size();
      for (int i = 0; i < length; i++) {
        sizeOfArray += values.get(i).length;
      }

      byte[] buf = new byte[sizeOfArray];
      int pos = 0;

      // TBinaryProtocol List header is element type followed by count
      buf[pos++] = 12; // TYPE_STRUCT
      buf[pos++] = (byte) ((length >>> 24L) & 0xff);
      buf[pos++] = (byte) ((length >>> 16L) & 0xff);
      buf[pos++] = (byte) ((length >>> 8L) & 0xff);
      buf[pos++] = (byte) (length & 0xff);

      // Then each struct is written one-at-a-time with no delimiters until done.
      for (int i = 0; i < length; ) {
        byte[] v = values.get(i++);
        System.arraycopy(v, 0, buf, pos, v.length);
        pos += v.length;
      }
      return buf;
    }
  },
  /**
   * This function simply concatenates the byte arrays.
   *
   * <p>The list of byte arrays represents a repeated (type 2) field. As such, each byte array is
   * expected to have a prefix of the field number, followed by the encoded length of the span,
   * finally, the actual span bytes.
   *
   * @see Encoding#PROTO3
   */
  PROTO3 {
    @Override public byte[] encode(List<byte[]> values) {
      int sizeOfArray = 0;
      int length = values.size();
      for (int i = 0; i < length; ) {
        sizeOfArray += values.get(i++).length;
      }

      byte[] buf = new byte[sizeOfArray];
      int pos = 0;
      for (int i = 0; i < length; ) {
        byte[] v = values.get(i++);
        System.arraycopy(v, 0, buf, pos, v.length);
        pos += v.length;
      }
      return buf;
    }
  };

  /**
   * Combines a list of encoded spans into an encoded list. For example, in thrift, this would be
   * length-prefixed, whereas in json, this would be comma-separated and enclosed by brackets.
   *
   * <p>The primary use of this is batch reporting spans. For example, spans are {@link
   * BytesEncoder#encode(Object) encoded} one-by-one into a queue. This queue is drained up to a byte
   * threshold. Then, the list is encoded with this function and reported out-of-process.
   */
  public abstract byte[] encode(List<byte[]> encodedSpans);

  public static BytesMessageEncoder forEncoding(Encoding encoding) {
    if (encoding == null) throw new NullPointerException("encoding == null");
    switch (encoding) {
      case JSON:
        return JSON;
      case PROTO3:
        return PROTO3;
      case THRIFT:
        return THRIFT;
      default:
        throw new UnsupportedOperationException(encoding.name());
    }
  }
}
