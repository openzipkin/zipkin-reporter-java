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
package zipkin.reporter.okhttp3;

import java.io.IOException;
import java.util.List;
import okhttp3.MediaType;
import okhttp3.RequestBody;
import okio.BufferedSink;
import zipkin.reporter.MessageEncoder;

enum RequestBodyMessageEncoder implements MessageEncoder<RequestBody> {
  THRIFT {
    @Override public int overheadInBytes(int spanCount) {
      return THRIFT_BYTES.overheadInBytes(spanCount);
    }

    @Override public RequestBody encode(List<byte[]> values) {
      return new ThriftRequestBody(values);
    }
  },
  JSON {
    @Override public int overheadInBytes(int spanCount) {
      return JSON_BYTES.overheadInBytes(spanCount);
    }

    @Override public RequestBody encode(List<byte[]> values) {
      return new JsonRequestBody(values);
    }
  };

  static abstract class StreamingRequestBody extends RequestBody {
    final MediaType contentType;
    final List<byte[]> values;
    final long contentLength;

    StreamingRequestBody(MessageEncoder<?> encoder, MediaType contentType, List<byte[]> values) {
      this.contentType = contentType;
      this.values = values;
      long contentLength = encoder.overheadInBytes(values.size());
      for (byte[] value : values) {
        contentLength += value.length;
      }
      this.contentLength = contentLength;
    }

    @Override public MediaType contentType() {
      return contentType;
    }

    @Override public long contentLength() {
      return contentLength;
    }
  }

  static final class ThriftRequestBody extends StreamingRequestBody {
    static final MediaType CONTENT_TYPE = MediaType.parse("application/x-thrift");

    ThriftRequestBody(List<byte[]> values) {
      super(MessageEncoder.THRIFT_BYTES, CONTENT_TYPE, values);
    }

    @Override public void writeTo(BufferedSink sink) throws IOException {
      // TBinaryProtocol List header is element type followed by count
      sink.writeByte(11 /* TYPE_STRUCT */);
      int length = values.size();
      sink.writeInt(length);
      for (int i = 0; i < length; i++) {
        sink.write(values.get(i));
      }
    }
  }

  static final class JsonRequestBody extends StreamingRequestBody {
    static final MediaType CONTENT_TYPE = MediaType.parse("application/json");

    JsonRequestBody(List<byte[]> values) {
      super(MessageEncoder.JSON_BYTES, CONTENT_TYPE, values);
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
}
