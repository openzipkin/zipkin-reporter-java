/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin2.reporter.brave;

import brave.handler.SpanHandler;
import java.util.List;
import zipkin2.Span;

class BasicUsageTest_Converting extends BasicUsageTest<ZipkinSpanHandler> {
  @Override ZipkinSpanHandler zipkinSpanHandler(List<Span> spans) {
    return (ZipkinSpanHandler) ZipkinSpanHandler.create(spans::add);
  }

  @Override void close(ZipkinSpanHandler handler) {
    handler.close();
  }

  @Override SpanHandler alwaysReportSpans(ZipkinSpanHandler handler) {
    return handler.toBuilder().alwaysReportSpans(true).build();
  }
}
