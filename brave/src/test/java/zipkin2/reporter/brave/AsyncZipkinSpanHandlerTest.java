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
package zipkin2.reporter.brave;

import brave.handler.MutableSpan;
import org.junit.jupiter.api.Test;
import zipkin2.reporter.BytesEncoder;
import zipkin2.reporter.Encoding;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

class AsyncZipkinSpanHandlerTest {
  @Test void build_protoNotYetSupported() {
    FakeSender sender = FakeSender.create().encoding(Encoding.PROTO3);
    AsyncZipkinSpanHandler.Builder builder = AsyncZipkinSpanHandler.newBuilder(sender);
    assertThrows(UnsupportedOperationException.class, builder::build);
  }

  /** Ready for custom format such as OTLP or Stackdriver. */
  @Test void build_customProtoEncoder() {
    FakeSender sender = FakeSender.create().encoding(Encoding.PROTO3);
    AsyncZipkinSpanHandler.Builder builder = AsyncZipkinSpanHandler.newBuilder(sender);
    BytesEncoder<MutableSpan> protoEncoder = new BytesEncoder<>() {
      @Override public Encoding encoding() {
        return Encoding.PROTO3;
      }

      @Override public int sizeInBytes(MutableSpan input) {
        return 0;
      }

      @Override public byte[] encode(MutableSpan input) {
        return new byte[0];
      }
    };

    try (AsyncZipkinSpanHandler spanReporter = builder.build(protoEncoder)) {
      assertThat(spanReporter).isNotNull();
    }
  }
}
