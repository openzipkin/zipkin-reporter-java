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

import brave.Tags;
import brave.handler.MutableSpan;
import brave.handler.SpanHandler;
import brave.propagation.TraceContext;
import java.util.ArrayList;
import java.util.Objects;
import org.junit.Before;
import org.junit.Test;
import zipkin2.reporter.Reporter;

import static brave.handler.SpanHandler.Cause.FINISHED;
import static brave.handler.SpanHandler.Cause.FLUSHED;
import static org.assertj.core.api.Assertions.assertThat;

public class ZipkinSpanHandlerTest {
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

  @Before public void init() {
    defaultSpan = new MutableSpan();
    defaultSpan.localServiceName("Aa");
    defaultSpan.localIp("1.2.3.4");
    defaultSpan.localPort(80);
    handler = new ZipkinSpanHandler(spans, Tags.ERROR, false);
  }

  @Test public void noopIsNoop() {
    assertThat(ZipkinSpanHandler.create(Reporter.NOOP))
        .isSameAs(SpanHandler.NOOP);
  }

  @Test public void equalsAndHashCode() {
    assertThat(handler)
        .hasSameHashCodeAs(spans)
        .isEqualTo(new ZipkinSpanHandler(spans, Tags.ERROR, false));

    ZipkinSpanHandler otherHandler = new ZipkinSpanHandler(spans::add, Tags.ERROR, false);

    assertThat(handler)
        .isNotEqualTo(otherHandler)
        .extracting(Objects::hashCode)
        .isNotEqualTo(otherHandler.hashCode());
  }

  @Test public void reportsSampledSpan() {
    MutableSpan span = new MutableSpan(context, null);
    handler.end(context, span, FINISHED);

    assertThat(spans.get(0)).isSameAs(span);
  }

  @Test public void reportsDebugSpan() {
    TraceContext context = TraceContext.newBuilder().traceId(1).spanId(2).debug(true).build();
    MutableSpan span = new MutableSpan(context, null);
    handler.end(context, span, FINISHED);

    assertThat(spans.get(0)).isSameAs(span);
  }

  @Test public void reportsFlushedSpan() {
    MutableSpan span = new MutableSpan(context, null);
    handler.end(context, span, FLUSHED);

    assertThat(spans.get(0)).isSameAs(span);
  }

  @Test public void doesnNotHandleAbandoned() {
    assertThat(handler.handlesAbandoned()).isFalse();
  }

  @Test public void doesntReportUnsampledSpan() {
    TraceContext context =
        TraceContext.newBuilder().traceId(1).spanId(2).sampled(false).sampledLocal(true).build();
    handler.end(context, new MutableSpan(context, null), FINISHED);

    assertThat(spans).isEmpty();
  }

  @Test public void alwaysReportSpans_reportsUnsampledSpan() {
    handler = new ZipkinSpanHandler(spans, Tags.ERROR, true);

    TraceContext context =
        TraceContext.newBuilder().traceId(1).spanId(2).sampled(false).sampledLocal(true).build();
    handler.end(context, new MutableSpan(context, null), FINISHED);

    assertThat(spans).isNotEmpty();
  }

  @Test public void alwaysReportSpans_doesNotHandleAbandoned() {
    handler = new ZipkinSpanHandler(spans, Tags.ERROR, true);
    assertThat(handler.handlesAbandoned()).isFalse();
  }
}
