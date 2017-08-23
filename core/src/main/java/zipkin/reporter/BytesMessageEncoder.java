/**
 * Copyright 2016-2017 The OpenZipkin Authors
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
package zipkin.reporter;

import java.util.List;

/**
 * Senders like Kafka use byte[] message encoding. This provides helpers to concatenate spans into a
 * list.
 */
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
  };

  /**
   * Combines a list of encoded spans into an encoded list. For example, in thrift, this would be
   * length-prefixed, whereas in json, this would be comma-separated and enclosed by brackets.
   *
   * <p>The primary use of this is batch reporting spans. For example, spans are {@link
   * Encoder#encode(Object) encoded} one-by-one into a queue. This queue is drained up to a byte
   * threshold. Then, the list is encoded with this function and reported out-of-process.
   */
  public abstract byte[] encode(List<byte[]> encodedSpans);

  public static BytesMessageEncoder forEncoding(Encoding encoding) {
    if (encoding == null) throw new NullPointerException("encoding == null");
    switch (encoding) {
      case JSON:
        return JSON;
      case THRIFT:
        return THRIFT;
      default:
        throw new UnsupportedOperationException(encoding.name());
    }
  }
}
