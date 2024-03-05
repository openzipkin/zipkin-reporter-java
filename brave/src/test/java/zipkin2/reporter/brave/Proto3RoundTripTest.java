/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin2.reporter.brave;

import zipkin2.codec.SpanBytesDecoder;

class Proto3RoundTripTest extends RoundTripTest {
  Proto3RoundTripTest() {
    super(MutableSpanBytesEncoder.PROTO3, SpanBytesDecoder.PROTO3);
  }
}
