/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin2.reporter.okhttp3;

import java.io.IOException;
import java.util.List;
import okhttp3.MediaType;
import okhttp3.RequestBody;
import okio.BufferedSink;
import zipkin2.reporter.Encoding;

enum RequestBodyMessageEncoder {
  JSON {
    @Override public RequestBody encode(List<byte[]> encodedSpans) {
      return new JsonRequestBody(encodedSpans);
    }
  },
  @Deprecated
  THRIFT {
    @Override RequestBody encode(List<byte[]> encodedSpans) {
      return new ThriftRequestBody(encodedSpans);
    }
  },
  PROTO3 {
    @Override RequestBody encode(List<byte[]> encodedSpans) {
      return new Protobuf3RequestBody(encodedSpans);
    }
  };

  static RequestBodyMessageEncoder forEncoding(Encoding encoding) {
    switch (encoding) {
      case JSON:
        return RequestBodyMessageEncoder.JSON;
      case THRIFT:
        return RequestBodyMessageEncoder.THRIFT;
      case PROTO3:
        return RequestBodyMessageEncoder.PROTO3;
      default:
        throw new UnsupportedOperationException("Unsupported encoding: " + encoding.name());
    }
  }

  static abstract class StreamingRequestBody extends RequestBody {
    final MediaType contentType;
    final List<byte[]> values;
    final long contentLength;

    StreamingRequestBody(Encoding encoding, List<byte[]> values) {
      this.contentType = MediaType.parse(encoding.mediaType());
      this.values = values;
      this.contentLength = encoding.listSizeInBytes(values);
    }

    @Override public MediaType contentType() {
      return contentType;
    }

    @Override public long contentLength() {
      return contentLength;
    }
  }

  static final class JsonRequestBody extends StreamingRequestBody {
    JsonRequestBody(List<byte[]> values) {
      super(Encoding.JSON, values);
    }

    @Override public void writeTo(BufferedSink sink) throws IOException {
      sink.writeByte('[');
      for (int i = 0, length = values.size(); i < length; ) {
        byte[] next = values.get(i++);
        sink.write(next);
        if (i < length) sink.writeByte(',');
      }
      sink.writeByte(']');
    }
  }

  static final class ThriftRequestBody extends StreamingRequestBody {
    ThriftRequestBody(List<byte[]> values) {
      super(Encoding.THRIFT, values);
    }

    @Override
    public void writeTo(BufferedSink sink) throws IOException {
      // TBinaryProtocol List header is element type followed by count
      int length = values.size();
      sink.writeByte(12); // TYPE_STRUCT
      sink.writeByte((length >>> 24L) & 0xff);
      sink.writeByte((length >>> 16L) & 0xff);
      sink.writeByte((length >>> 8L) & 0xff);
      sink.writeByte(length & 0xff);

      // Then each struct is written one-at-a-time with no delimiters until done.
      for (int i = 0; i < length; ) {
        byte[] next = values.get(i++);
        sink.write(next);
      }
    }
  }

  static final class Protobuf3RequestBody extends StreamingRequestBody {
    Protobuf3RequestBody(List<byte[]> values) {
      super(Encoding.PROTO3, values);
    }

    @Override public void writeTo(BufferedSink sink) throws IOException {
      for (int i = 0, length = values.size(); i < length; ) {
        byte[] next = values.get(i++);
        sink.write(next);
      }
    }
  }

  abstract RequestBody encode(List<byte[]> encodedSpans);
}
