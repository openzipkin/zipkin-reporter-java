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

import java.util.List;
import zipkin.reporter.Encoding;
import zipkin.reporter.MessageEncoder;
import zipkin.reporter.Sender.MessageEncoding;

public final class ThriftBytesMessageEncoder
    implements MessageEncoder<byte[], byte[]>, MessageEncoding {

  @Override public Encoding encoding() {
    return Encoding.THRIFT;
  }

  /** Encoding overhead is thrift type plus 32-bit length prefix */
  @Override public int overheadInBytes(int spanCount) {
    return 5;
  }

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
}
