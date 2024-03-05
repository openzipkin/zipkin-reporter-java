/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin2.reporter.brave;

import brave.handler.SpanHandler;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.extension.RegisterExtension;
import zipkin2.Span;
import zipkin2.junit5.ZipkinExtension;
import zipkin2.reporter.okhttp3.OkHttpSender;

class BasicUsageTest_Async extends BasicUsageTest<AsyncZipkinSpanHandler> {
  @RegisterExtension public ZipkinExtension zipkin = new ZipkinExtension();

  OkHttpSender sender = OkHttpSender.create(zipkin.httpUrl() + "/api/v2/spans");

  @Override AsyncZipkinSpanHandler zipkinSpanHandler(List<Span> spans) {
    return AsyncZipkinSpanHandler.newBuilder(sender)
      .messageTimeout(0, TimeUnit.MILLISECONDS) // don't spawn a thread
      .build();
  }

  @Override void close(AsyncZipkinSpanHandler handler) {
    handler.close();
  }

  @Override SpanHandler alwaysReportSpans(AsyncZipkinSpanHandler handler) {
    return handler.toBuilder().alwaysReportSpans(true).build();
  }

  @Override void triggerReport() {
    zipkinSpanHandler.flush();
    for (List<Span> trace : zipkin.getTraces()) {
      spans.addAll(trace);
    }
  }

  @Override public void close() {
    super.close();
    zipkinSpanHandler.close();
    sender.close();
  }
}
