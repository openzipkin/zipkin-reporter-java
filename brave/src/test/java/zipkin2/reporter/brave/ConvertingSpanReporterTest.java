/*
 * Copyright 2016-2024 The OpenZipkin Authors
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
import brave.propagation.TraceContext;
import java.util.ArrayList;
import java.util.Objects;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import zipkin2.Annotation;
import zipkin2.Endpoint;
import zipkin2.Span;
import zipkin2.reporter.Reporter;

import static brave.Span.Kind.CLIENT;
import static brave.Span.Kind.CONSUMER;
import static brave.Span.Kind.PRODUCER;
import static brave.Span.Kind.SERVER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;

class ConvertingSpanReporterTest {
  static class ListSpanReporter extends ArrayList<Span> implements Reporter<Span> {
    @Override public void report(Span span) {
      add(span);
    }
  }

  ListSpanReporter spans = new ListSpanReporter();
  ConvertingSpanReporter spanReporter;
  TraceContext context = TraceContext.newBuilder().traceId(1).spanId(2).sampled(true).build();
  MutableSpan defaultSpan;

  @BeforeEach void init() {
    defaultSpan = new MutableSpan(context, null);
    defaultSpan.localServiceName("Aa");
    defaultSpan.localIp("1.2.3.4");
    defaultSpan.localPort(80);
    spanReporter = new ConvertingSpanReporter(spans, Tags.ERROR);
  }

  @Test void generateKindMap() {
    assertThat(ConvertingSpanReporter.generateKindMap()).containsExactly(
      entry(CLIENT, Span.Kind.CLIENT),
      entry(SERVER, Span.Kind.SERVER),
      entry(PRODUCER, Span.Kind.PRODUCER),
      entry(CONSUMER, Span.Kind.CONSUMER)
    );
  }

  @Test void equalsAndHashCode() {
    assertThat(spanReporter)
      .hasSameHashCodeAs(spans)
      .isEqualTo(new ConvertingSpanReporter(spans, Tags.ERROR));

    ConvertingSpanReporter otherReporter = new ConvertingSpanReporter(spans::add, Tags.ERROR);

    assertThat(spanReporter)
      .isNotEqualTo(otherReporter)
      .extracting(Objects::hashCode)
      .isNotEqualTo(otherReporter.hashCode());
  }

  @Test void convertsSampledSpan() {
    MutableSpan span = new MutableSpan(context, null);
    spanReporter.report(span);

    assertThat(spans.get(0)).usingRecursiveComparison().isEqualTo(
      Span.newBuilder()
        .traceId("1")
        .id("2")
        .build()
    );
  }

  @Test void convertsDebugSpan() {
    TraceContext context = TraceContext.newBuilder().traceId(1).spanId(2).debug(true).build();
    MutableSpan span = new MutableSpan(context, null);
    spanReporter.report(span);

    assertThat(spans.get(0)).usingRecursiveComparison().isEqualTo(
      Span.newBuilder()
        .traceId("0000000000000001")
        .id("0000000000000002")
        .debug(true)
        .build()
    );
  }

  @Test void minimumDurationIsOne() {
    MutableSpan span = new MutableSpan(context, null);

    span.startTimestamp(1L);
    span.finishTimestamp(1L);

    spanReporter.report(span);
    assertThat(spans.get(0).duration()).isEqualTo(1L);
  }

  @Test void replacesTag() {
    MutableSpan span = new MutableSpan(context, null);

    span.tag("1", "1");
    span.tag("foo", "bar");
    span.tag("2", "2");
    span.tag("foo", "baz");
    span.tag("3", "3");

    spanReporter.report(span);
    assertThat(spans.get(0).tags()).containsOnly(
      entry("1", "1"),
      entry("foo", "baz"),
      entry("2", "2"),
      entry("3", "3")
    );
  }

  Throwable ERROR = new RuntimeException();

  @Test void backfillsErrorTag() {
    MutableSpan span = new MutableSpan(context, null);

    span.error(ERROR);

    spanReporter.report(span);

    assertThat(spans.get(0).tags())
      .containsOnly(entry("error", "RuntimeException"));
  }

  @Test void doesntOverwriteErrorTag() {
    MutableSpan span = new MutableSpan(context, null);

    span.error(ERROR);
    span.tag("error", "");

    spanReporter.report(span);

    assertThat(spans.get(0).tags())
      .containsOnly(entry("error", ""));
  }

  @Test void addsAnnotations() {
    MutableSpan span = new MutableSpan(context, null);

    span.startTimestamp(1L);
    span.annotate(2L, "foo");
    span.finishTimestamp(2L);

    spanReporter.report(span);

    assertThat(spans.get(0).annotations())
      .containsOnly(Annotation.create(2L, "foo"));
  }

  @Test void finished_client() {
    finish(brave.Span.Kind.CLIENT, Span.Kind.CLIENT);
  }

  @Test void finished_server() {
    finish(brave.Span.Kind.SERVER, Span.Kind.SERVER);
  }

  @Test void finished_producer() {
    finish(brave.Span.Kind.PRODUCER, Span.Kind.PRODUCER);
  }

  @Test void finished_consumer() {
    finish(brave.Span.Kind.CONSUMER, Span.Kind.CONSUMER);
  }

  void finish(brave.Span.Kind braveKind, Span.Kind span2Kind) {
    MutableSpan span = new MutableSpan(context, null);
    span.kind(braveKind);
    span.startTimestamp(1L);
    span.finishTimestamp(2L);

    spanReporter.report(span);

    Span zipkinSpan = spans.get(0);
    assertThat(zipkinSpan.annotations()).isEmpty();
    assertThat(zipkinSpan.timestamp()).isEqualTo(1L);
    assertThat(zipkinSpan.duration()).isEqualTo(1L);
    assertThat(zipkinSpan.kind()).isEqualTo(span2Kind);
  }

  @Test void flushed_client() {
    flush(brave.Span.Kind.CLIENT, Span.Kind.CLIENT);
  }

  @Test void flushed_server() {
    flush(brave.Span.Kind.SERVER, Span.Kind.SERVER);
  }

  @Test void flushed_producer() {
    flush(brave.Span.Kind.PRODUCER, Span.Kind.PRODUCER);
  }

  @Test void flushed_consumer() {
    flush(brave.Span.Kind.CONSUMER, Span.Kind.CONSUMER);
  }

  void flush(brave.Span.Kind braveKind, Span.Kind span2Kind) {
    MutableSpan span = new MutableSpan(context, null);
    span.kind(braveKind);
    span.startTimestamp(1L);
    span.finishTimestamp(0L);

    spanReporter.report(span);

    Span zipkinSpan = spans.get(0);
    assertThat(zipkinSpan.annotations()).isEmpty();
    assertThat(zipkinSpan.timestamp()).isEqualTo(1L);
    assertThat(zipkinSpan.duration()).isNull();
    assertThat(zipkinSpan.kind()).isEqualTo(span2Kind);
  }

  @Test void remoteEndpoint() {
    MutableSpan span = new MutableSpan(context, null);

    Endpoint endpoint = Endpoint.newBuilder()
      .serviceName("fooService")
      .ip("1.2.3.4")
      .port(80)
      .build();

    span.kind(CLIENT);
    span.remoteServiceName(endpoint.serviceName());
    span.remoteIpAndPort(endpoint.ipv4(), endpoint.port());
    span.startTimestamp(1L);
    span.finishTimestamp(2L);

    spanReporter.report(span);

    assertThat(spans.get(0).remoteEndpoint())
      .isEqualTo(endpoint);
  }

  // This prevents the server startTimestamp from overwriting the client one on the collector
  @Test void writeTo_sharedStatus() {
    MutableSpan span = new MutableSpan(context, null);

    span.setShared();
    span.startTimestamp(1L);
    span.kind(SERVER);
    span.finishTimestamp(2L);

    spanReporter.report(span);

    assertThat(spans.get(0).shared())
      .isTrue();
  }

  @Test void flushUnstartedNeitherSetsTimestampNorDuration() {
    MutableSpan flushed = new MutableSpan(context, null);
    flushed.finishTimestamp(0L);

    spanReporter.report(flushed);

    assertThat(spans.get(0)).extracting(Span::timestampAsLong, Span::durationAsLong)
      .allSatisfy(u -> assertThat(u).isEqualTo(0L));
  }
}
