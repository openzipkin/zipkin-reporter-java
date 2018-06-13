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
package zipkin2.reporter.okhttp3;

import java.io.IOException;
import java.util.List;
import okhttp3.MediaType;
import okhttp3.RequestBody;
import okio.BufferedSink;
import zipkin2.codec.Encoding;

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

  static abstract class StreamingRequestBody extends RequestBody {
    final MediaType contentType;
    final List<byte[]> values;
    final long contentLength;

    StreamingRequestBody(Encoding encoding, MediaType contentType, List<byte[]> values) {
      this.contentType = contentType;
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
    static final MediaType CONTENT_TYPE = MediaType.parse("application/json");

    JsonRequestBody(List<byte[]> values) {
      super(Encoding.JSON, CONTENT_TYPE, values);
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
    static final MediaType CONTENT_TYPE = MediaType.parse("application/x-thrift");

    ThriftRequestBody(List<byte[]> values) {
      super(Encoding.THRIFT, CONTENT_TYPE, values);
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
    static final MediaType CONTENT_TYPE = MediaType.parse("application/x-protobuf");

    Protobuf3RequestBody(List<byte[]> values) {
      super(Encoding.PROTO3, CONTENT_TYPE, values);
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
