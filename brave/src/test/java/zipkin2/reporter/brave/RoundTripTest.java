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

import brave.Tag;
import brave.handler.MutableSpan;
import brave.propagation.TraceContext;
import org.junit.jupiter.api.Test;
import zipkin2.Endpoint;
import zipkin2.Span;
import zipkin2.TestObjects;
import zipkin2.codec.BytesDecoder;
import zipkin2.reporter.BytesEncoder;

import static org.assertj.core.api.Assertions.assertThat;
import static zipkin2.reporter.brave.ZipkinSpanConverter.CLIENT_SPAN;

abstract class RoundTripTest {

  final BytesEncoder<MutableSpan> encoder;
  final BytesDecoder<Span> zipkinDecoder;

  RoundTripTest(BytesEncoder<MutableSpan> encoder, BytesDecoder<Span> zipkinDecoder) {
    this.encoder = encoder;
    this.zipkinDecoder = zipkinDecoder;
  }

  @Test void clientSpan() {
    assertDecodableByZipkin(CLIENT_SPAN, TestObjects.CLIENT_SPAN);
  }

  @Test void specialCharacters() {
    MutableSpan span = newSpan();
    span.name("\u2028 and \u2029");
    span.localServiceName("\"foo");
    span.tag("hello \n", "\t\b");
    span.annotate(1L, "\uD83D\uDCA9");

    assertDecodableByZipkin(span, newZipkinSpan()
      .name("\u2028 and \u2029")
      .localEndpoint(Endpoint.newBuilder().serviceName("\"foo").build())
      .putTag("hello \n", "\t\b")
      .addAnnotation(1L, "\uD83D\uDCA9").build());
  }

  @Test void errorTag() {
    MutableSpan span = newSpan();
    span.tag("a", "1");
    span.tag("error", "true");
    span.tag("b", "2");

    assertDecodableByZipkin(span, newZipkinSpan()
      .putTag("a", "1")
      .putTag("error", "true")
      .putTag("b", "2").build());
  }

  @Test void error() {
    MutableSpan span = newSpan();
    span.tag("a", "1");
    span.tag("b", "2");
    span.error(new RuntimeException("ice cream"));

    assertDecodableByZipkin(span, newZipkinSpan()
      .putTag("a", "1")
      .putTag("error", "ice cream")
      .putTag("b", "2").build());
  }

  @Test void existingErrorTagWins() {
    MutableSpan span = newSpan();
    span.tag("a", "1");
    span.tag("error", "true");
    span.tag("b", "2");
    span.error(new RuntimeException("ice cream"));

    assertDecodableByZipkin(span, newZipkinSpan()
      .putTag("a", "1")
      .putTag("error", "true")
      .putTag("b", "2").build());
  }

  @Test void differentErrorTagName() {
    BytesEncoder<MutableSpan> encoder =
      MutableSpanBytesEncoder.create(this.encoder.encoding(), new Tag<Throwable>("exception") {
        @Override protected String parseValue(Throwable input, TraceContext context) {
          return input.getMessage();
        }
      });

    MutableSpan span = newSpan();
    span.tag("a", "1");
    span.tag("error", "true");
    span.tag("b", "2");
    span.error(new RuntimeException("ice cream"));

    assertDecodableByZipkin(encoder, span, newZipkinSpan()
      .putTag("a", "1")
      .putTag("error", "true")
      .putTag("b", "2")
      .putTag("exception", "ice cream")
      .build());
  }

  @Test void localIp_mappedIpv4() {
    String mappedIpv4 = "::FFFF:43.0.192.2";

    MutableSpan span = newSpan();
    span.localIp(mappedIpv4);

    assertDecodableByZipkin(span, newZipkinSpan()
      .localEndpoint(Endpoint.newBuilder().ip(mappedIpv4).build())
      .build());
  }

  @Test void localIp_compatIpv4() {
    String compatIpv4 = "::0000:43.0.192.2";

    MutableSpan span = newSpan();
    span.localIp(compatIpv4);

    assertDecodableByZipkin(span, newZipkinSpan()
      .localEndpoint(Endpoint.newBuilder().ip(compatIpv4).build())
      .build());
  }

  @Test void localIp_notMappedIpv4() {
    String invalid = "::ffef:43.0.192.2";

    MutableSpan span = newSpan();
    span.localIp(invalid);

    // Prove that the zipkin logic knows this is invalid
    Endpoint endpoint = Endpoint.newBuilder().ip(invalid).build();
    assertThat(endpoint.ipv4()).isNull();
    assertThat(endpoint.ipv6()).isNull();

    // Prove we didn't encode the local IP!
    assertDecodableByZipkin(span, newZipkinSpan().build());
  }

  void assertDecodableByZipkin(MutableSpan span, Span zSpan) {
    assertDecodableByZipkin(encoder, span, zSpan);
  }

  void assertDecodableByZipkin(BytesEncoder<MutableSpan> encoder, MutableSpan span, Span zSpan) {
    int size = encoder.sizeInBytes(span);
    byte[] encoded = encoder.encode(span);
    assertThat(encoded).hasSize(size);

    // Zipkin doesn't maintain the same tag order, so we need to use equals.
    Span decoded = zipkinDecoder.decodeOne(encoded);
    assertThat(decoded).isEqualTo(zSpan);
  }

  static MutableSpan newSpan() {
    MutableSpan span = new MutableSpan();
    span.traceId("1");
    span.id("2");
    return span;
  }

  static Span.Builder newZipkinSpan() {
    return Span.newBuilder().traceId("1").id("2");
  }
}
