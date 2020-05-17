/*
 * Copyright 2016-2020 The OpenZipkin Authors
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
