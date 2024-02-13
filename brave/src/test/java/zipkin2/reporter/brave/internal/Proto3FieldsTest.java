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

import java.util.List;
import org.junit.jupiter.api.Test;
import zipkin2.reporter.brave.internal.Proto3Fields.BooleanField;
import zipkin2.reporter.brave.internal.Proto3Fields.BytesField;
import zipkin2.reporter.brave.internal.Proto3Fields.Fixed64Field;
import zipkin2.reporter.brave.internal.Proto3Fields.Utf8Field;
import zipkin2.reporter.brave.internal.Proto3Fields.VarintField;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.failBecauseExceptionWasNotThrown;
import static zipkin2.reporter.brave.internal.Proto3Fields.Field;
import static zipkin2.reporter.brave.internal.Proto3Fields.HexField;
import static zipkin2.reporter.brave.internal.Proto3Fields.WIRETYPE_FIXED64;
import static zipkin2.reporter.brave.internal.Proto3Fields.WIRETYPE_LENGTH_DELIMITED;
import static zipkin2.reporter.brave.internal.Proto3Fields.WIRETYPE_VARINT;

class Proto3FieldsTest {
  byte[] bytes = new byte[2048]; // bigger than needed to test sizeInBytes
  WriteBuffer buf = new WriteBuffer(bytes);

  /** Shows we can reliably look at a byte zero to tell if we are decoding proto3 repeated fields. */
  @Test void field_key_fieldOneLengthDelimited() {
    Field field = new Field(1 << 3 | WIRETYPE_LENGTH_DELIMITED);
    assertThat(field.key)
      .isEqualTo(0b00001010) // (field_number << 3) | wire_type = 1 << 3 | 2
      .isEqualTo(10); // for sanity of those looking at debugger, 4th bit + 2nd bit = 10
    assertThat(field.fieldNumber)
      .isEqualTo(1);
    assertThat(field.wireType)
      .isEqualTo(WIRETYPE_LENGTH_DELIMITED);
  }

  @Test void varint_sizeInBytes() {
    VarintField field = new VarintField(1 << 3 | WIRETYPE_VARINT);

    assertThat(field.sizeInBytes(0))
      .isZero();
    assertThat(field.sizeInBytes(0xffffffff))
      .isEqualTo(0
        + 1 /* tag of varint field */ + 5 // max size of varint32
      );

    assertThat(field.sizeInBytes(0L))
      .isZero();
    assertThat(field.sizeInBytes(0xffffffffffffffffL))
      .isEqualTo(0
        + 1 /* tag of varint field */ + 10 // max size of varint64
      );
  }

  @Test void boolean_sizeInBytes() {
    BooleanField field = new BooleanField(1 << 3 | WIRETYPE_VARINT);

    assertThat(field.sizeInBytes(false))
      .isZero();
    assertThat(field.sizeInBytes(true))
      .isEqualTo(0
        + 1 /* tag of varint field */ + 1 // size of 1
      );
  }

  @Test void utf8_sizeInBytes() {
    Utf8Field field = new Utf8Field(1 << 3 | WIRETYPE_LENGTH_DELIMITED);
    assertThat(field.sizeInBytes("12345678"))
      .isEqualTo(0
        + 1 /* tag of string field */ + 1 /* len */ + 8 // 12345678
      );
  }

  @Test void fixed64_sizeInBytes() {
    Fixed64Field field = new Fixed64Field(1 << 3 | WIRETYPE_FIXED64);
    assertThat(field.sizeInBytes(Long.MIN_VALUE))
      .isEqualTo(9);
  }

  @Test void supportedFields() {
    for (Field field : List.of(
      new VarintField(128 << 3 | WIRETYPE_VARINT),
      new BooleanField(128 << 3 | WIRETYPE_VARINT),
      new HexField(128 << 3 | WIRETYPE_LENGTH_DELIMITED),
      new Utf8Field(128 << 3 | WIRETYPE_LENGTH_DELIMITED),
      new BytesField(128 << 3 | WIRETYPE_LENGTH_DELIMITED),
      new Fixed64Field(128 << 3 | WIRETYPE_FIXED64)
    )) {
      assertThat(Field.fieldNumber(field.key, 1))
        .isEqualTo(field.fieldNumber);
      assertThat(Field.wireType(field.key, 1))
        .isEqualTo(field.wireType);
    }
  }

  @Test void fieldNumber_malformed() {
    try {
      Field.fieldNumber(0, 2);
      failBecauseExceptionWasNotThrown(IllegalArgumentException.class);
    } catch (IllegalArgumentException e) {
      assertThat(e)
        .hasMessage("Malformed: fieldNumber was zero at byte 2");
    }
  }

  @Test void wireType_unsupported() {
    for (int unsupported : List.of(3, 4, 6)) {
      try {
        Field.wireType(1 << 3 | unsupported, 2);
        failBecauseExceptionWasNotThrown(IllegalArgumentException.class);
      } catch (IllegalArgumentException e) {
        assertThat(e)
          .hasMessage("Malformed: invalid wireType " + unsupported + " at byte 2");
      }
    }
  }
}
