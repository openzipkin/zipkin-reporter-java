/*
 * Copyright 2016-2018 The OpenZipkin Authors
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
package zipkin2.reporter.libthrift;

import java.util.List;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TField;
import org.apache.thrift.protocol.TList;
import org.apache.thrift.protocol.TMessage;
import org.apache.thrift.protocol.TMessageType;
import org.apache.thrift.protocol.TProtocolUtil;
import org.apache.thrift.protocol.TType;

import static org.apache.thrift.TApplicationException.BAD_SEQUENCE_ID;
import static org.apache.thrift.TApplicationException.MISSING_RESULT;

public final class InternalScribeCodec { // public for zipkin-finagle

  static final TField CATEGORY_FIELD_DESC = new TField("category", TType.STRING, (short) 1);
  static final TField MESSAGE_FIELD_DESC = new TField("message", TType.STRING, (short) 2);
  static final TField MESSAGES_FIELD_DESC = new TField("messages", TType.LIST, (short) 1);

  public static int messageSizeInBytes(byte[] category, int spanSizeInBytes) {
    int sizeInBytes = 4 + (4 + 3 /* Log */) + 4; // strict write message begin
    sizeInBytes += 3; // FieldBegin
    sizeInBytes += 5; // ListBegin
    sizeInBytes += sizeOfLogEntry(category, spanSizeInBytes);
    sizeInBytes += 1; // FieldStop
    return sizeInBytes;
  }

  public static int messageSizeInBytes(byte[] category, List<byte[]> encodedSpans) {
    int sizeInBytes = 4 + (4 + 3 /* Log */) + 4; // strict write message begin
    sizeInBytes += 3; // FieldBegin
    sizeInBytes += 5; // ListBegin
    for (byte[] encodedSpan : encodedSpans) {
      sizeInBytes += sizeOfLogEntry(category, encodedSpan.length);
    }
    sizeInBytes += 1; // FieldStop
    return sizeInBytes;
  }

  public static void writeLogRequest(
      byte[] category, List<byte[]> encodedSpans, int seqid, TBinaryProtocol oprot)
      throws TException {
    oprot.writeMessageBegin(new TMessage("Log", TMessageType.CALL, seqid));
    oprot.writeFieldBegin(MESSAGES_FIELD_DESC);
    oprot.writeListBegin(new TList(TType.STRUCT, encodedSpans.size()));
    for (byte[] encodedSpan : encodedSpans) write(category, encodedSpan, oprot);
    oprot.writeFieldStop();
  }

  /** Returns false if the scribe response was try later. */
  public static boolean readLogResponse(int seqid, TBinaryProtocol iprot) throws TException {
    TMessage msg = iprot.readMessageBegin();
    if (msg.type == TMessageType.EXCEPTION) {
      throw TApplicationException.readFrom(iprot);
    } else if (msg.seqid != seqid) {
      throw new TApplicationException(BAD_SEQUENCE_ID, "Log failed: out of sequence response");
    }
    return parseResponse(iprot);
  }

  static boolean parseResponse(TBinaryProtocol iprot) throws TException {
    iprot.readStructBegin();
    TField schemeField;
    while ((schemeField = iprot.readFieldBegin()).type != TType.STOP) {
      if (schemeField.id == 0 /* SUCCESS */ && schemeField.type == TType.I32) {
        return iprot.readI32() == 0;
      } else {
        TProtocolUtil.skip(iprot, schemeField.type);
      }
    }
    throw new TApplicationException(MISSING_RESULT, "Log failed: unknown result");
  }

  static int sizeOfLogEntry(byte[] category, int spanSizeInBytes) {
    int sizeInBytes = 3 + 4 + category.length;
    sizeInBytes += 3 + 4 + base64SizeInBytes(spanSizeInBytes);
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

  static final byte[] MAP = {
    'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S',
    'T', 'U', 'V', 'W', 'X', 'Y', 'Z', 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l',
    'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z', '0', '1', '2', '3', '4',
    '5', '6', '7', '8', '9', '+', '/'
  };

  /**
   * Adapted from okio.Base64 as JRE 6 doesn't have a base64 encoder
   *
   * <p>Original author: Alexander Y. Kleymenov
   */
  static byte[] base64(byte[] in) {
    int length = base64SizeInBytes(in.length);
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
    assert index == out.length : "index " + index + " != out.length " + out.length;
    return out;
  }

  static int base64SizeInBytes(int sizeInBytes) {
    int result = sizeInBytes * 4 / 3;
    int remainder = sizeInBytes * 4 % 3;
    return remainder == 0 ? result : result + (4 - remainder);
  }
}
