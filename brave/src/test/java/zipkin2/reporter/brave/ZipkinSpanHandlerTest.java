/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin2.reporter.brave;

import brave.Tags;
import brave.handler.MutableSpan;
import brave.handler.SpanHandler;
import brave.propagation.TraceContext;
import java.util.ArrayList;
import java.util.Objects;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import zipkin2.reporter.Reporter;

import static brave.handler.SpanHandler.Cause.FINISHED;
import static brave.handler.SpanHandler.Cause.FLUSHED;
import static org.assertj.core.api.Assertions.assertThat;

class ZipkinSpanHandlerTest {
  static class ListMutableSpanReporter extends ArrayList<MutableSpan>
      implements Reporter<MutableSpan> {
    @Override public void report(MutableSpan span) {
      add(span);
    }
  }

  ListMutableSpanReporter spans = new ListMutableSpanReporter();
  ZipkinSpanHandler handler;
  TraceContext context = TraceContext.newBuilder().traceId(1).spanId(2).sampled(true).build();
  MutableSpan defaultSpan;

  @BeforeEach void init() {
    defaultSpan = new MutableSpan();
    defaultSpan.localServiceName("Aa");
    defaultSpan.localIp("1.2.3.4");
    defaultSpan.localPort(80);
    handler = new ZipkinSpanHandler(spans, Tags.ERROR, false);
  }

  @Test void noopIsNoop() {
    assertThat(ZipkinSpanHandler.create(Reporter.NOOP))
        .isSameAs(SpanHandler.NOOP);
  }

  @Test void equalsAndHashCode() {
    assertThat(handler)
        .hasSameHashCodeAs(spans)
        .isEqualTo(new ZipkinSpanHandler(spans, Tags.ERROR, false));

    ZipkinSpanHandler otherHandler = new ZipkinSpanHandler(spans::add, Tags.ERROR, false);

    assertThat(handler)
        .isNotEqualTo(otherHandler)
        .extracting(Objects::hashCode)
        .isNotEqualTo(otherHandler.hashCode());
  }

  @Test void reportsSampledSpan() {
    MutableSpan span = new MutableSpan(context, null);
    handler.end(context, span, FINISHED);

    assertThat(spans.get(0)).isSameAs(span);
  }

  @Test void reportsDebugSpan() {
    TraceContext context = TraceContext.newBuilder().traceId(1).spanId(2).debug(true).build();
    MutableSpan span = new MutableSpan(context, null);
    handler.end(context, span, FINISHED);

    assertThat(spans.get(0)).isSameAs(span);
  }

  @Test void reportsFlushedSpan() {
    MutableSpan span = new MutableSpan(context, null);
    handler.end(context, span, FLUSHED);

    assertThat(spans.get(0)).isSameAs(span);
  }

  @Test void doesnNotHandleAbandoned() {
    assertThat(handler.handlesAbandoned()).isFalse();
  }

  @Test void doesntReportUnsampledSpan() {
    TraceContext context =
        TraceContext.newBuilder().traceId(1).spanId(2).sampled(false).sampledLocal(true).build();
    handler.end(context, new MutableSpan(context, null), FINISHED);

    assertThat(spans).isEmpty();
  }

  @Test void alwaysReportSpans_reportsUnsampledSpan() {
    handler = new ZipkinSpanHandler(spans, Tags.ERROR, true);

    TraceContext context =
        TraceContext.newBuilder().traceId(1).spanId(2).sampled(false).sampledLocal(true).build();
    handler.end(context, new MutableSpan(context, null), FINISHED);

    assertThat(spans).isNotEmpty();
  }

  @Test void alwaysReportSpans_doesNotHandleAbandoned() {
    handler = new ZipkinSpanHandler(spans, Tags.ERROR, true);
    assertThat(handler.handlesAbandoned()).isFalse();
  }
}
