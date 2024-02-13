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

import brave.Span.Kind;
import brave.Tags;
import brave.handler.MutableSpan;
import org.junit.jupiter.api.Test;
import zipkin2.reporter.brave.internal.ZipkinProto3Fields.AnnotationField;
import zipkin2.reporter.brave.internal.ZipkinProto3Fields.EndpointField;
import zipkin2.reporter.brave.internal.ZipkinProto3Fields.SpanField;
import zipkin2.reporter.brave.internal.ZipkinProto3Fields.TagField;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.atIndex;
import static zipkin2.reporter.brave.internal.Proto3Fields.WIRETYPE_LENGTH_DELIMITED;

class ZipkinProto3FieldsTest {
  static final SpanField SPAN_FIELD = new SpanField(Tags.ERROR);
  byte[] bytes = new byte[2048]; // bigger than needed to test sizeInBytes
  WriteBuffer buf = new WriteBuffer(bytes);

  /** A map entry is an embedded messages: one for field the key and one for the value */
  @Test void tag_sizeInBytes() {
    TagField field = new TagField(1 << 3 | WIRETYPE_LENGTH_DELIMITED);
    assertThat(field.sizeInBytes("123", "56789"))
      .isEqualTo(0
        + 1 /* tag of embedded key field */ + 1 /* len */ + 3
        + 1 /* tag of embedded value field  */ + 1 /* len */ + 5
        + 1 /* tag of map entry field */ + 1 /* len */
      );
  }

  @Test void annotation_sizeInBytes() {
    AnnotationField field = new AnnotationField(1 << 3 | WIRETYPE_LENGTH_DELIMITED);
    assertThat(field.sizeInBytes(1L, "12345678"))
      .isEqualTo(0
        + 1 /* tag of timestamp field */ + 8 /* 8 byte number */
        + 1 /* tag of value field */ + 1 /* len */ + 8 // 12345678
        + 1 /* tag of annotation field */ + 1 /* len */
      );
  }

  @Test void endpoint_sizeInBytes() {
    EndpointField field = new EndpointField(1 << 3 | WIRETYPE_LENGTH_DELIMITED);

    assertThat(field.sizeInBytes("12345678", "192.168.99.101", 80))
      .isEqualTo(0
        + 1 /* tag of servicename field */ + 1 /* len */ + 8 // 12345678
        + 1 /* tag of ipv4 field */ + 1 /* len */ + 4 // octets in ipv4
        + 1 /* tag of port field */ + 1 /* small varint */
        + 1 /* tag of endpoint field */ + 1 /* len */
      );

    assertThat(field.sizeInBytes("12345678", "2001:db8::c001", 80))
      .isEqualTo(0
        + 1 /* tag of servicename field */ + 1 /* len */ + 8 // 12345678
        + 1 /* tag of ipv6 field */ + 1 /* len */ + 16 // octets in ipv6
        + 1 /* tag of port field */ + 1 /* small varint */
        + 1 /* tag of endpoint field */ + 1 /* len */
      );
  }

  @Test void span_write_startsWithFieldInListOfSpans() {
    SPAN_FIELD.write(buf, newSpan());

    assertThat(bytes).startsWith(
      0b00001010 /* span key */, 20 /* bytes for length of the span */
    );
  }

  @Test void span_write_writesIds() {
    SPAN_FIELD.write(buf, newSpan());
    assertThat(bytes).startsWith(
      0b00001010 /* span key */, 20 /* bytes for length of the span */,
      0b00001010 /* trace ID key */, 8 /* bytes for 64-bit trace ID */,
      0, 0, 0, 0, 0, 0, 0, 1, // hex trace ID
      0b00011010 /* span ID key */, 8 /* bytes for 64-bit span ID */,
      0, 0, 0, 0, 0, 0, 0, 2 // hex span ID
    );
    assertThat(buf.pos())
      .isEqualTo(3 * 2 /* overhead of three fields */ + 2 * 8 /* 64-bit fields */)
      .isEqualTo(22); // easier math on the next test
  }

  @Test void span_write_omitsEmptyEndpoints() {
    SPAN_FIELD.write(buf, newSpan());

    assertThat(buf.pos())
      .isEqualTo(22);
  }

  @Test void span_write_kind() {
    MutableSpan span = newSpan();
    span.kind(Kind.PRODUCER);
    SPAN_FIELD.write(buf, span);
    assertThat(bytes)
      .contains(0b0100000, atIndex(22)) // (field_number << 3) | wire_type = 4 << 3 | 0
      .contains(0b0000011, atIndex(23)); // producer's index is 3
  }

  @Test void span_write_debug() {
    MutableSpan span = newSpan();
    span.setDebug();
    SPAN_FIELD.write(buf, span);

    assertThat(bytes)
      .contains(0b01100000, atIndex(buf.pos() - 2)) // (field_number << 3) | wire_type = 12 << 3 | 0
      .contains(1, atIndex(buf.pos() - 1)); // true
  }

  @Test void span_write_shared() {
    MutableSpan span = newSpan();
    span.kind(Kind.SERVER);
    span.setShared();
    SPAN_FIELD.write(buf, span);

    assertThat(bytes)
      .contains(0b01101000, atIndex(buf.pos() - 2)) // (field_number << 3) | wire_type = 13 << 3 | 0
      .contains(1, atIndex(buf.pos() - 1)); // true
  }

  static MutableSpan newSpan() {
    MutableSpan span = new MutableSpan();
    span.traceId("1");
    span.id("2");
    return span;
  }
}
