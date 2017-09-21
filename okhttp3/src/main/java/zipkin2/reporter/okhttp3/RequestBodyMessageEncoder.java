/**
 * Copyright 2016-2017 The OpenZipkin Authors
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
    @Override public RequestBody encode(List<byte[]> values) {
      return new JsonRequestBody(values);
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

  abstract RequestBody encode(List<byte[]> encodedSpans);
}
