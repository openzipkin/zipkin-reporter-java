/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin2.reporter.brave;

import brave.Tag;
import brave.Tags;
import brave.handler.MutableSpan;
import brave.propagation.TraceContext;
import org.junit.jupiter.api.Test;
import zipkin2.reporter.BytesEncoder;
import zipkin2.reporter.Encoding;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MutableSpanBytesEncoderTest {
  @Test void forEncoding() {
    assertThat(MutableSpanBytesEncoder.forEncoding(Encoding.JSON))
      .isSameAs(MutableSpanBytesEncoder.JSON_V2);
    assertThat(MutableSpanBytesEncoder.forEncoding(Encoding.PROTO3))
      .isSameAs(MutableSpanBytesEncoder.PROTO3);
    assertThatThrownBy(() -> MutableSpanBytesEncoder.forEncoding(Encoding.THRIFT))
      .isInstanceOf(UnsupportedOperationException.class)
      .hasMessage("THRIFT is not yet a built-in encoder");
  }

  Tag<Throwable> iceCream = new Tag<>("exception") {
    @Override protected String parseValue(Throwable throwable, TraceContext traceContext) {
      return "ice cream";
    }
  };

  @Test void create_json() {
    // doesn't allocate on defaults
    assertThat(MutableSpanBytesEncoder.create(Encoding.JSON, Tags.ERROR))
      .isSameAs(MutableSpanBytesEncoder.JSON_V2);

    MutableSpan span = new MutableSpan();
    span.traceId("1");
    span.id("2");
    span.error(new OutOfMemoryError("out of memory"));

    // Default makes a tag named error
    assertThat(new String(MutableSpanBytesEncoder.JSON_V2.encode(span), UTF_8))
      .isEqualTo("{\"traceId\":\"0000000000000001\",\"id\":\"0000000000000002\",\"tags\":{\"error\":\"out of memory\"}}");


    // but, using create, you can override with something else.
    BytesEncoder<MutableSpan> iceCreamEncoder =
      MutableSpanBytesEncoder.create(Encoding.JSON, iceCream);
    assertThat(new String(iceCreamEncoder.encode(span), UTF_8))
      .isEqualTo("{\"traceId\":\"0000000000000001\",\"id\":\"0000000000000002\",\"tags\":{\"exception\":\"ice cream\"}}");
  }

  @Test void create_proto3() {
    // doesn't allocate on defaults
    assertThat(MutableSpanBytesEncoder.create(Encoding.PROTO3, Tags.ERROR))
      .isSameAs(MutableSpanBytesEncoder.PROTO3);

    MutableSpan span = new MutableSpan();
    span.traceId("1");
    span.id("2");
    span.error(new OutOfMemoryError("out of memory"));

    // Default makes a tag named error
    assertThat(new String(MutableSpanBytesEncoder.PROTO3.encode(span), UTF_8))
      .contains("out of memory");

    // but, using create, you can override with something else.
    BytesEncoder<MutableSpan> iceCreamEncoder =
      MutableSpanBytesEncoder.create(Encoding.PROTO3, iceCream);
    assertThat(new String(iceCreamEncoder.encode(span), UTF_8))
      .contains("ice cream");
  }

  @Test void create_unsupported() {
    assertThatThrownBy(() -> MutableSpanBytesEncoder.create(Encoding.THRIFT, iceCream))
      .isInstanceOf(UnsupportedOperationException.class)
      .hasMessage("THRIFT is not yet a built-in encoder");
  }
}
