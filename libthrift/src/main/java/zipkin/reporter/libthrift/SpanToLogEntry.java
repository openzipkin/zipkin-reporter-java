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
package zipkin.reporter.libthrift;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TField;
import org.apache.thrift.protocol.TType;

final class SpanToLogEntry {
  static final TField CATEGORY_FIELD_DESC = new TField("category", TType.STRING, (short) 1);
  static final TField MESSAGE_FIELD_DESC = new TField("message", TType.STRING, (short) 2);

  static int sizeOfLogEntry(byte[] category, byte[] span) {
    int sizeInBytes = 5 + 4 + category.length;
    sizeInBytes += 5 + 4 + base64SizeInBytes(span);
    sizeInBytes++; // stop
    return sizeInBytes;
  }

  static void write(byte[] category, byte[] span, TBinaryProtocol oprot) throws TException {
    oprot.writeFieldBegin(CATEGORY_FIELD_DESC);
    oprot.writeI32(category.length);
    oprot.getTransport().write(category, 0, category.length);

    oprot.writeFieldBegin(MESSAGE_FIELD_DESC);
    byte[] base64 = base64(span);
    oprot.writeI32(base64.length);
    oprot.getTransport().write(base64, 0, base64.length);
    oprot.writeFieldStop();
  }

  private static final byte[] MAP = new byte[] {
      'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S',
      'T', 'U', 'V', 'W', 'X', 'Y', 'Z', 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l',
      'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z', '0', '1', '2', '3', '4',
      '5', '6', '7', '8', '9', '+', '/'
  };

  /**
   * Adapted from okio.Base64 as JRE 6 doesn't have a base64Url encoder
   *
   * <p>Original author: Alexander Y. Kleymenov
   */
  static byte[] base64(byte[] in) {
    int length = base64SizeInBytes(in);
    byte[] out = new byte[length];
    int index = 0, end = in.length - in.length % 3;
    for (int i = 0; i < end; i += 3) {
      out[index++] = MAP[(in[i] & 0xff) >> 2];
      out[index++] = MAP[((in[i] & 0x03) << 4) | ((in[i + 1] & 0xff) >> 4)];
      out[index++] = MAP[((in[i + 1] & 0x0f) << 2) | ((in[i + 2] & 0xff) >> 6)];
      out[index++] = MAP[(in[i + 2] & 0x3f)];
    }
    switch (in.length % 3) {
      case 1:
        out[index++] = MAP[(in[end] & 0xff) >> 2];
        out[index++] = MAP[(in[end] & 0x03) << 4];
        out[index++] = '=';
        out[index++] = '=';
        break;
      case 2:
        out[index++] = MAP[(in[end] & 0xff) >> 2];
        out[index++] = MAP[((in[end] & 0x03) << 4) | ((in[end + 1] & 0xff) >> 4)];
        out[index++] = MAP[((in[end + 1] & 0x0f) << 2)];
        out[index++] = '=';
        break;
    }
    return out;
  }

  private static int base64SizeInBytes(byte[] in) {
    return (in.length + 2) * 4 / 3;
  }
}
