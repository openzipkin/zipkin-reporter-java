/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin2.reporter.brave.internal;

import brave.Tag;
import brave.handler.MutableSpan;
import zipkin2.reporter.brave.internal.Proto3Fields.BooleanField;
import zipkin2.reporter.brave.internal.Proto3Fields.IPv4Field;
import zipkin2.reporter.brave.internal.Proto3Fields.IPv6Field;
import zipkin2.reporter.brave.internal.Proto3Fields.Utf8Field;
import zipkin2.reporter.internal.Nullable;

import static zipkin2.reporter.brave.internal.Proto3Fields.Fixed64Field;
import static zipkin2.reporter.brave.internal.Proto3Fields.HexField;
import static zipkin2.reporter.brave.internal.Proto3Fields.LengthDelimitedField;
import static zipkin2.reporter.brave.internal.Proto3Fields.VarintField;
import static zipkin2.reporter.brave.internal.Proto3Fields.WIRETYPE_FIXED64;
import static zipkin2.reporter.brave.internal.Proto3Fields.WIRETYPE_LENGTH_DELIMITED;
import static zipkin2.reporter.brave.internal.Proto3Fields.WIRETYPE_VARINT;
import static zipkin2.reporter.brave.internal.Proto3Fields.sizeOfLengthDelimitedField;

/**
 * Stripped version of {@linkplain zipkin2.internal.Proto3ZipkinFields}, without decoding logic.
 *
 * <p>Also, brave has no structs for things like annotations or endpoints, so we have to flatten
 * some logic.
 *
 * <p>Finally, brave has more involved error logic, so this logic was derived from
 * {@link brave.internal.codec.ZipkinV2JsonWriter}.
 */
//@Immutable
final class ZipkinProto3Fields {
  static class EndpointField extends Proto3Fields.Field {
    static final int SERVICE_NAME_KEY = (1 << 3) | WIRETYPE_LENGTH_DELIMITED;
    static final int IPV4_KEY = (2 << 3) | WIRETYPE_LENGTH_DELIMITED;
    static final int IPV6_KEY = (3 << 3) | WIRETYPE_LENGTH_DELIMITED;
    static final int PORT_KEY = (4 << 3) | WIRETYPE_VARINT;

    static final Utf8Field SERVICE_NAME = new Utf8Field(SERVICE_NAME_KEY);
    static final IPv4Field IPV4 = new IPv4Field(IPV4_KEY);
    static final IPv6Field IPV6 = new IPv6Field(IPV6_KEY);
    static final VarintField PORT = new VarintField(PORT_KEY);

    EndpointField(int key) {
      super(key);
      assert wireType == WIRETYPE_LENGTH_DELIMITED;
    }

    int sizeInBytes(@Nullable String serviceName, @Nullable String ip, int port) {
      int sizeOfValue = sizeOfValue(serviceName, ip, port);
      // size is possibly zero, so don't write an empty field!
      return sizeOfValue > 0 ? sizeOfLengthDelimitedField(sizeOfValue) : 0;
    }

    static int sizeOfValue(@Nullable String serviceName, @Nullable String ip, int port) {
      int sizeInBytes = 0;
      sizeInBytes += SERVICE_NAME.sizeInBytes(serviceName);
      // MutableSpan unwraps any Ipv4 from a mapped or compatability mode IPv6.
      if (ip != null && ip.indexOf('.') != -1) {
        sizeInBytes += IPV4.sizeInBytes(ip);
      } else {
        sizeInBytes += IPV6.sizeInBytes(ip);
      }
      sizeInBytes += PORT.sizeInBytes(port);
      return sizeInBytes;
    }

    void write(WriteBuffer b, @Nullable String serviceName, @Nullable String ip, int port) {
      int sizeOfValue = sizeOfValue(serviceName, ip, port);
      if (sizeOfValue == 0) return; // special-case empty endpoint
      b.writeByte(key);
      b.writeVarint(sizeOfValue); // length prefix
      SERVICE_NAME.write(b, serviceName);
      // MutableSpan unwraps any Ipv4 from a mapped or compatability mode IPv6.
      if (ip != null && ip.indexOf('.') != -1) {
        IPV4.write(b, ip);
      } else {
        IPV6.write(b, ip);
      }
      PORT.write(b, port);
    }
  }

  static class AnnotationField extends Proto3Fields.Field {
    static final int TIMESTAMP_KEY = (1 << 3) | WIRETYPE_FIXED64;
    static final int VALUE_KEY = (2 << 3) | WIRETYPE_LENGTH_DELIMITED;

    static final Fixed64Field TIMESTAMP = new Fixed64Field(TIMESTAMP_KEY);
    static final Utf8Field VALUE = new Utf8Field(VALUE_KEY);

    AnnotationField(int key) {
      super(key);
      assert wireType == WIRETYPE_LENGTH_DELIMITED;
    }

    int sizeInBytes(long timestamp, String value) {
      int sizeOfValue = sizeOfValue(timestamp, value);
      return sizeOfLengthDelimitedField(sizeOfValue);
    }

    static int sizeOfValue(long timestamp, String value) {
      return TIMESTAMP.sizeInBytes(timestamp) + VALUE.sizeInBytes(value);
    }

    final void write(WriteBuffer b, long timestamp, String value) {
      int sizeOfValue = sizeOfValue(timestamp, value);
      b.writeByte(key);
      b.writeVarint(sizeOfValue); // length prefix
      TIMESTAMP.write(b, timestamp);
      VALUE.write(b, value);
    }
  }

  static final class TagField extends Proto3Fields.Field {
    // map<string,string> in proto is a special field with key, value
    static final int KEY_KEY = (1 << 3) | WIRETYPE_LENGTH_DELIMITED;
    static final int VALUE_KEY = (2 << 3) | WIRETYPE_LENGTH_DELIMITED;

    static final Utf8Field KEY = new Utf8Field(KEY_KEY);
    static final Utf8Field VALUE = new Utf8Field(VALUE_KEY);

    TagField(int key) {
      super(key);
      assert wireType == WIRETYPE_LENGTH_DELIMITED;
    }

    int sizeInBytes(String key, String value) {
      int sizeInBytes = sizeOfValue(key, value);
      return sizeOfLengthDelimitedField(sizeInBytes);
    }

    static int sizeOfValue(String key, String value) {
      return KEY.sizeInBytes(key) + VALUE.sizeInBytes(value);
    }

    void write(WriteBuffer b, String key, String value) {
      if (value == null) return;
      int sizeOfValue = sizeOfValue(key, value);
      b.writeByte(this.key);
      b.writeVarint(sizeOfValue); // length prefix
      KEY.write(b, key);
      VALUE.write(b, value);
    }
  }

  /** This is the only field in the ListOfSpans type */
  static class SpanField extends LengthDelimitedField<MutableSpan> {
    static final int TRACE_ID_KEY = (1 << 3) | WIRETYPE_LENGTH_DELIMITED;
    static final int PARENT_ID_KEY = (2 << 3) | WIRETYPE_LENGTH_DELIMITED;
    static final int ID_KEY = (3 << 3) | WIRETYPE_LENGTH_DELIMITED;
    static final int KIND_KEY = (4 << 3) | WIRETYPE_VARINT;
    static final int NAME_KEY = (5 << 3) | WIRETYPE_LENGTH_DELIMITED;
    static final int TIMESTAMP_KEY = (6 << 3) | WIRETYPE_FIXED64;
    static final int DURATION_KEY = (7 << 3) | WIRETYPE_VARINT;
    static final int LOCAL_ENDPOINT_KEY = (8 << 3) | WIRETYPE_LENGTH_DELIMITED;
    static final int REMOTE_ENDPOINT_KEY = (9 << 3) | WIRETYPE_LENGTH_DELIMITED;
    static final int ANNOTATION_KEY = (10 << 3) | WIRETYPE_LENGTH_DELIMITED;
    static final int TAG_KEY = (11 << 3) | WIRETYPE_LENGTH_DELIMITED;
    static final int DEBUG_KEY = (12 << 3) | WIRETYPE_VARINT;
    static final int SHARED_KEY = (13 << 3) | WIRETYPE_VARINT;

    static final HexField TRACE_ID = new HexField(TRACE_ID_KEY);
    static final HexField PARENT_ID = new HexField(PARENT_ID_KEY);
    static final HexField ID = new HexField(ID_KEY);
    static final VarintField KIND = new VarintField(KIND_KEY);
    static final Utf8Field NAME = new Utf8Field(NAME_KEY);
    static final Fixed64Field TIMESTAMP = new Fixed64Field(TIMESTAMP_KEY);
    static final VarintField DURATION = new VarintField(DURATION_KEY);
    static final EndpointField LOCAL_ENDPOINT = new EndpointField(LOCAL_ENDPOINT_KEY);
    static final EndpointField REMOTE_ENDPOINT = new EndpointField(REMOTE_ENDPOINT_KEY);
    static final AnnotationField ANNOTATION = new AnnotationField(ANNOTATION_KEY);
    static final TagField TAG = new TagField(TAG_KEY);
    static final BooleanField DEBUG = new BooleanField(DEBUG_KEY);
    static final BooleanField SHARED = new BooleanField(SHARED_KEY);

    final Tag<Throwable> errorTag;

    SpanField(Tag<Throwable> errorTag) {
      super((1 << 3) | WIRETYPE_LENGTH_DELIMITED);
      if (errorTag == null) throw new NullPointerException("errorTag == null");
      this.errorTag = errorTag;
    }

    @Override int sizeOfValue(MutableSpan span) {
      int sizeInBytes = TRACE_ID.sizeInBytes(span.traceId());
      sizeInBytes += PARENT_ID.sizeInBytes(span.parentId());
      sizeInBytes += ID.sizeInBytes(span.id());
      sizeInBytes += KIND.sizeInBytes(span.kind() != null ? 1 : 0);
      sizeInBytes += NAME.sizeInBytes(span.name());
      if (span.startTimestamp() != 0L) {
        sizeInBytes += TIMESTAMP.sizeInBytes(span.startTimestamp());
        if (span.finishTimestamp() != 0L) {
          sizeInBytes += DURATION.sizeInBytes(span.finishTimestamp() - span.startTimestamp());
        }
      }

      sizeInBytes +=
        LOCAL_ENDPOINT.sizeInBytes(span.localServiceName(), span.localIp(), span.localPort());
      sizeInBytes +=
        REMOTE_ENDPOINT.sizeInBytes(span.remoteServiceName(), span.remoteIp(), span.remotePort());

      int annotationLength = span.annotationCount();
      for (int i = 0; i < annotationLength; i++) {
        sizeInBytes +=
          ANNOTATION.sizeInBytes(span.annotationTimestampAt(i), span.annotationValueAt(i));
      }

      int tagCount = span.tagCount();
      String errorValue = errorTag.value(span.error(), null);
      String errorTagName = errorValue != null ? errorTag.key() : null;
      boolean writeError = errorTagName != null;
      if (tagCount > 0 || writeError) {
        for (int i = 0; i < tagCount; i++) {
          String key = span.tagKeyAt(i);
          if (writeError && key.equals(errorTagName)) writeError = false;
          sizeInBytes += TAG.sizeInBytes(key, span.tagValueAt(i));
        }
        if (writeError) {
          sizeInBytes += TAG.sizeInBytes(errorTagName, errorValue);
        }
      }

      sizeInBytes += DEBUG.sizeInBytes(Boolean.TRUE.equals(span.debug()));
      sizeInBytes += SHARED.sizeInBytes(Boolean.TRUE.equals(span.shared()));
      return sizeInBytes;
    }

    @Override void writeValue(WriteBuffer b, MutableSpan span) {
      TRACE_ID.write(b, span.traceId());
      PARENT_ID.write(b, span.parentId());
      ID.write(b, span.id());
      KIND.write(b, toByte(span.kind()));
      NAME.write(b, span.name());

      if (span.startTimestamp() != 0L) {
        TIMESTAMP.write(b, span.startTimestamp());
        if (span.finishTimestamp() != 0L) {
          DURATION.write(b, span.finishTimestamp() - span.startTimestamp());
        }
      }

      LOCAL_ENDPOINT.write(b, span.localServiceName(), span.localIp(), span.localPort());
      REMOTE_ENDPOINT.write(b, span.remoteServiceName(), span.remoteIp(), span.remotePort());

      int annotationLength = span.annotationCount();
      for (int i = 0; i < annotationLength; i++) {
        ANNOTATION.write(b, span.annotationTimestampAt(i), span.annotationValueAt(i));
      }

      int tagCount = span.tagCount();
      String errorValue = errorTag.value(span.error(), null);
      String errorTagName = errorValue != null ? errorTag.key() : null;
      boolean writeError = errorTagName != null;
      if (tagCount > 0 || writeError) {
        for (int i = 0; i < tagCount; i++) {
          String key = span.tagKeyAt(i);
          if (writeError && key.equals(errorTagName)) writeError = false;
          TAG.write(b, key, span.tagValueAt(i));
        }
        if (writeError) {
          TAG.write(b, errorTagName, errorValue);
        }
      }

      SpanField.DEBUG.write(b, Boolean.TRUE.equals(span.debug()));
      SpanField.SHARED.write(b, Boolean.TRUE.equals(span.shared()));
    }

    // in java, there's no zero index for unknown
    int toByte(brave.Span.Kind kind) {
      return kind != null ? kind.ordinal() + 1 : 0;
    }
  }
}
