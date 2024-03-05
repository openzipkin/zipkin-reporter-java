/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin2.reporter.brave;

import brave.handler.SpanHandler;
import zipkin2.Span;
import zipkin2.reporter.Reporter;

final class ConvertingZipkinSpanHandler extends ZipkinSpanHandler {

  @Override public ZipkinSpanHandler.Builder toBuilder() {
    return new Builder(((ConvertingSpanReporter) spanReporter).delegate);
  }

  static final class Builder extends ZipkinSpanHandler.Builder {
    final Reporter<Span> spanReporter;

    Builder(Reporter<Span> spanReporter) {
      this.spanReporter = spanReporter;
    }

    // SpanHandler not ZipkinSpanHandler as it can coerce to NOOP
    @Override public SpanHandler build() {
      if (spanReporter == Reporter.NOOP) return SpanHandler.NOOP;
      return new ConvertingZipkinSpanHandler(this);
    }
  }

  ConvertingZipkinSpanHandler(Builder builder) {
    super(new ConvertingSpanReporter(builder.spanReporter, builder.errorTag),
        builder.errorTag, builder.alwaysReportSpans);
  }
}
