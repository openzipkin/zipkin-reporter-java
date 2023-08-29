/*
 * Copyright 2016-2023 The OpenZipkin Authors
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
package zipkin2.reporter.otlp;

import brave.handler.SpanHandler;
import io.opentelemetry.proto.trace.v1.TracesData;
import zipkin2.reporter.Reporter;

final class ConvertingOtlpSpanHandler extends OtlpSpanHandler {

  @Override
  public Builder toBuilder() {
    return new Builder(((OtlpConvertingSpanReporter) spanReporter).delegate);
  }

  static final class Builder extends OtlpSpanHandler.Builder {
    final Reporter<?> spanReporter;

    Builder(Reporter<TracesData> spanReporter) {
      this.spanReporter = spanReporter;
    }

    // SpanHandler not OtlpSpanHandler as it can coerce to NOOP
    @Override public SpanHandler build() {
      if (spanReporter == Reporter.NOOP) return SpanHandler.NOOP;
      return new ConvertingOtlpSpanHandler(this);
    }
  }

  @SuppressWarnings("unchecked")
  ConvertingOtlpSpanHandler(Builder builder) {
    super(new OtlpConvertingSpanReporter((Reporter<TracesData>) builder.spanReporter, builder.errorTag),
        builder.errorTag, builder.alwaysReportSpans);
  }
}
