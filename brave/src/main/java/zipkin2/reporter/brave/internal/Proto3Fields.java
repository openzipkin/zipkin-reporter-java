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
package zipkin2.reporter.brave.internal;

import zipkin2.reporter.internal.Nullable;

import static zipkin2.reporter.brave.internal.WriteBuffer.utf8SizeInBytes;
import static zipkin2.reporter.brave.internal.WriteBuffer.varintSizeInBytes;

/**
 * Stripped version of {@linkplain zipkin2.internal.Proto3Fields}, without decoding logic.
 *
 * <p>Everything here assumes the field numbers are less than 16, implying a 1 byte tag.
 */
//@Immutable
final class Proto3Fields {
  /**
   * Define the wire types we use.
   *
   * <p>See https://developers.google.com/protocol-buffers/docs/encoding#structure
   */
  static final int
    WIRETYPE_VARINT = 0,
    WIRETYPE_FIXED64 = 1,
    WIRETYPE_LENGTH_DELIMITED = 2;

  static class Field {
    final int fieldNumber;
    final int wireType;
    /**
     * "Each key in the streamed message is a varint with the value {@code (field_number << 3) |
     * wire_type}"
     *
     * <p>See https://developers.google.com/protocol-buffers/docs/encoding#structure
     */
    final int key;

    Field(int key) {
      this(key >>> 3, key & (1 << 3) - 1, key);
    }

    Field(int fieldNumber, int wireType, int key) {
      this.fieldNumber = fieldNumber;
      this.wireType = wireType;
      this.key = key;
    }

    static int fieldNumber(int key, int byteL) {
      int fieldNumber = key >>> 3;
      if (fieldNumber != 0) return fieldNumber;
      throw new IllegalArgumentException("Malformed: fieldNumber was zero at byte " + byteL);
    }

    static int wireType(int key, int byteL) {
      int wireType = key & (1 << 3) - 1;
      if (wireType != 0 && wireType != 1 && wireType != 2 && wireType != 5) {
        throw new IllegalArgumentException(
          "Malformed: invalid wireType " + wireType + " at byte " + byteL);
      }
      return wireType;
    }
  }

  static abstract class LengthDelimitedField<T> extends Field {
    LengthDelimitedField(int key) {
      super(key);
      assert wireType == WIRETYPE_LENGTH_DELIMITED;
    }

    final int sizeInBytes(T value) {
      if (value == null) return 0;
      int sizeOfValue = sizeOfValue(value);
      return sizeOfLengthDelimitedField(sizeOfValue);
    }

    final void write(WriteBuffer b, T value) {
      if (value == null) return;
      int sizeOfValue = sizeOfValue(value);
      b.writeByte(key);
      b.writeVarint(sizeOfValue); // length prefix
      writeValue(b, value);
    }

    abstract int sizeOfValue(T value);

    abstract void writeValue(WriteBuffer b, T value);
  }

  static class BytesField extends LengthDelimitedField<byte[]> {
    BytesField(int key) {
      super(key);
    }

    @Override int sizeOfValue(byte[] bytes) {
      return bytes.length;
    }

    @Override void writeValue(WriteBuffer b, byte[] bytes) {
      b.write(bytes);
    }
  }

  static class IPv4Field extends LengthDelimitedField<String> {
    IPv4Field(int key) {
      super(key);
    }

    @Override int sizeOfValue(@Nullable String ipv4) {
      return ipv4 != null ? 4 : 0;
    }

    @Override void writeValue(WriteBuffer b, String ipv4) {
      IpWriter.writeIpv4Bytes(b, ipv4);
    }
  }

  static class IPv6Field extends LengthDelimitedField<String> {
    IPv6Field(int key) {
      super(key);
    }

    @Override int sizeOfValue(String ipv6) {
      return ipv6 != null ? 16 : 0;
    }

    @Override void writeValue(WriteBuffer b, String ipv6) {
      IpWriter.writeIpv6Bytes(b, ipv6);
    }
  }

  static class HexField extends LengthDelimitedField<String> {
    HexField(int key) {
      super(key);
    }

    @Override int sizeOfValue(String hex) {
      if (hex == null) return 0;
      return hex.length() / 2;
    }

    @Override void writeValue(WriteBuffer b, String hex) {
      // similar logic to okio.ByteString.decodeHex
      for (int i = 0, length = hex.length(); i < length; i++) {
        int d1 = decodeLowerHex(hex.charAt(i++)) << 4;
        int d2 = decodeLowerHex(hex.charAt(i));
        b.writeByte((byte) (d1 + d2));
      }
    }

    static int decodeLowerHex(char c) {
      if (c >= '0' && c <= '9') return c - '0';
      if (c >= 'a' && c <= 'f') return c - 'a' + 10;
      throw new AssertionError("not lowerHex " + c); // bug
    }
  }

  static class Utf8Field extends LengthDelimitedField<String> {
    Utf8Field(int key) {
      super(key);
    }

    @Override int sizeOfValue(String utf8) {
      return utf8 != null ? utf8SizeInBytes(utf8) : 0;
    }

    @Override void writeValue(WriteBuffer b, String utf8) {
      b.writeUtf8(utf8);
    }
  }

  static final class Fixed64Field extends Field {
    Fixed64Field(int key) {
      super(key);
      assert wireType == WIRETYPE_FIXED64;
    }

    void write(WriteBuffer b, long number) {
      if (number == 0) return;
      b.writeByte(key);
      b.writeLongLe(number);
    }

    int sizeInBytes(long number) {
      if (number == 0) return 0;
      return 1 + 8; // tag + 8 byte number
    }
  }

  static class VarintField extends Field {
    VarintField(int key) {
      super(key);
      assert wireType == WIRETYPE_VARINT;
    }

    int sizeInBytes(int number) {
      return number != 0 ? 1 + varintSizeInBytes(number) : 0; // tag + varint
    }

    void write(WriteBuffer b, int number) {
      if (number == 0) return;
      b.writeByte(key);
      b.writeVarint(number);
    }

    int sizeInBytes(long number) {
      return number != 0 ? 1 + varintSizeInBytes(number) : 0; // tag + varint
    }

    void write(WriteBuffer b, long number) {
      if (number == 0) return;
      b.writeByte(key);
      b.writeVarint(number);
    }
  }

  static final class BooleanField extends Field {
    BooleanField(int key) {
      super(key);
      assert wireType == WIRETYPE_VARINT;
    }

    int sizeInBytes(boolean bool) {
      return bool ? 2 : 0; // tag + varint
    }

    void write(WriteBuffer b, boolean bool) {
      if (!bool) return;
      b.writeByte(key);
      b.writeByte(1);
    }
  }

  static int sizeOfLengthDelimitedField(int sizeInBytes) {
    return 1 + varintSizeInBytes(sizeInBytes) + sizeInBytes; // tag + len + bytes
  }
}
