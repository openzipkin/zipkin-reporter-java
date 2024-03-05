/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin2.reporter.brave;

import zipkin2.codec.SpanBytesDecoder;

class JsonV2RoundTripTest extends RoundTripTest {
  JsonV2RoundTripTest() {
    super(MutableSpanBytesEncoder.JSON_V2, SpanBytesDecoder.JSON_V2);
  }
}
