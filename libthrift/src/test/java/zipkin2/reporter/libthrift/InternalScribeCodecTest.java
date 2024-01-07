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
package zipkin2.reporter.libthrift;

import java.io.ByteArrayOutputStream;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TIOStreamTransport;
import org.junit.jupiter.api.Test;
import zipkin2.Endpoint;
import zipkin2.Span;
import zipkin2.reporter.SpanBytesEncoder;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static zipkin2.TestObjects.CLIENT_SPAN;
import static zipkin2.TestObjects.TODAY;
import static zipkin2.TestObjects.UTF_8;

class InternalScribeCodecTest {
  @Test void base64_matches() {
    // testing every padding value
    for (String input : Arrays.asList("abc", "abcd", "abcd", "abcde")) {
      byte[] base64 = Base64.getEncoder().encode(input.getBytes(UTF_8));
      assertThat(InternalScribeCodec.base64SizeInBytes(input.length())).isEqualTo(base64.length);
      assertThat(InternalScribeCodec.base64(input.getBytes(UTF_8))).containsExactly(base64);
    }
  }

  @Test void sendsSpansExpectedMetrics() throws Exception {
    byte[] thrift = SpanBytesEncoder.THRIFT.encode(CLIENT_SPAN);
    List<byte[]> encodedSpans = asList(thrift, thrift);

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    TBinaryProtocol prot = new TBinaryProtocol(new TIOStreamTransport(out));

    InternalScribeCodec.writeLogRequest(ScribeClient.category, encodedSpans, 1, prot);

    assertThat(InternalScribeCodec.messageSizeInBytes(ScribeClient.category, encodedSpans))
        .isEqualTo(out.size());
  }

  @Test void sendsSpanExpectedMetrics() throws Exception {
    byte[] thrift = SpanBytesEncoder.THRIFT.encode(CLIENT_SPAN);
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    TBinaryProtocol prot = new TBinaryProtocol(new TIOStreamTransport(out));

    InternalScribeCodec.writeLogRequest(ScribeClient.category, Arrays.asList(thrift), 1, prot);

    assertThat(InternalScribeCodec.messageSizeInBytes(ScribeClient.category, Arrays.asList(thrift)))
        .isEqualTo(out.size());

    assertThat(InternalScribeCodec.messageSizeInBytes(ScribeClient.category, thrift.length))
        .isEqualTo(out.size());
  }

  @Test void base64SizeInBytes() {
    Endpoint web = Endpoint.newBuilder().serviceName("web").ip("127.0.0.1").build();

    Span span1 =
        Span.newBuilder()
            .traceId("d2f9288a2904503d")
            .id("d2f9288a2904503d")
            .name("get")
            .timestamp(TODAY * 1000)
            .duration(1000L)
            .kind(Span.Kind.SERVER)
            .localEndpoint(web)
            .build();

    Span span2 =
        span1
            .toBuilder()
            .kind(Span.Kind.CLIENT)
            .traceId("d2f9288a2904503d")
            .parentId("d2f9288a2904503d")
            .id("0f28590523a46541")
            .build();

    byte[] thrift1 = SpanBytesEncoder.THRIFT.encode(span1);
    assertThat(InternalScribeCodec.base64(thrift1))
        .hasSize(InternalScribeCodec.base64SizeInBytes(thrift1.length));

    byte[] thrift2 = SpanBytesEncoder.THRIFT.encode(span2);
    assertThat(InternalScribeCodec.base64(thrift2))
        .hasSize(InternalScribeCodec.base64SizeInBytes(thrift2.length));
  }
}
