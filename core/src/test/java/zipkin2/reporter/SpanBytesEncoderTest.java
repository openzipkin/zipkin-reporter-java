/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin2.reporter;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class SpanBytesEncoderTest {
  @Test void forEncoding() {
    assertThat(SpanBytesEncoder.forEncoding(Encoding.JSON))
      .isSameAs(SpanBytesEncoder.JSON_V2);
    assertThat(SpanBytesEncoder.forEncoding(Encoding.PROTO3))
      .isSameAs(SpanBytesEncoder.PROTO3);
    assertThat(SpanBytesEncoder.forEncoding(Encoding.THRIFT))
      .isSameAs(SpanBytesEncoder.THRIFT);
  }
}
