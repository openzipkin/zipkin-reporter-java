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

import java.util.concurrent.TimeUnit;

import brave.Span.Kind;
import brave.Tag;
import brave.handler.MutableSpan;
import brave.handler.MutableSpan.AnnotationConsumer;
import brave.handler.MutableSpan.TagConsumer;
import brave.handler.SpanHandler;
import com.google.protobuf.ByteString;
import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.trace.v1.ResourceSpans;
import io.opentelemetry.proto.trace.v1.ResourceSpans.Builder;
import io.opentelemetry.proto.trace.v1.ScopeSpans;
import io.opentelemetry.proto.trace.v1.Span;
import io.opentelemetry.proto.trace.v1.Span.SpanKind;
import io.opentelemetry.proto.trace.v1.TracesData;
import zipkin2.reporter.Reporter;

final class OtlpConvertingSpanReporter implements Reporter<MutableSpan> {

  final Reporter<TracesData> delegate;
  final Tag<Throwable> errorTag;

  OtlpConvertingSpanReporter(Reporter<TracesData> delegate, Tag<Throwable> errorTag) {
    this.delegate = delegate;
    this.errorTag = errorTag;
  }

  @Override public void report(MutableSpan span) {
    maybeAddErrorTag(span);
    TracesData converted = convert(span);
    delegate.report(converted);
  }

  static TracesData convert(MutableSpan span) {
    TracesData.Builder tracesDataBuilder = TracesData.newBuilder();
    ResourceSpans.Builder resourceSpansBuilder = ResourceSpans.newBuilder();
    ScopeSpans.Builder scopeSpanBuilder = ScopeSpans.newBuilder();
    Span.Builder spanBuilder = Span.newBuilder()
        .setTraceId(ByteString.fromHex(span.traceId()))
      .setSpanId(ByteString.fromHex(span.id()))
      .setName(span.name());
    if (span.parentId() != null) {
        spanBuilder.setParentSpanId(ByteString.fromHex(span.parentId()));
    }
    long start = span.startTimestamp();
    long finish = span.finishTimestamp();
    spanBuilder.setStartTimeUnixNano(TimeUnit.MICROSECONDS.toNanos(start));
    if (start != 0 && finish != 0L) {
      spanBuilder.setEndTimeUnixNano(TimeUnit.MICROSECONDS.toNanos(finish));
    }
    Kind kind = span.kind();
    if (kind != null) {
      switch (kind) {
        case CLIENT:
          spanBuilder.setKind(SpanKind.SPAN_KIND_CLIENT);
          break;
        case SERVER:
          spanBuilder.setKind(SpanKind.SPAN_KIND_SERVER);
          break;
        case PRODUCER:
          spanBuilder.setKind(SpanKind.SPAN_KIND_PRODUCER);
          break;
        case CONSUMER:
          spanBuilder.setKind(SpanKind.SPAN_KIND_CONSUMER);
          break;
        default:
          spanBuilder.setKind(SpanKind.SPAN_KIND_INTERNAL); //TODO: Should it work like this?
      }
    }
    // TODO: Consider moving to a "stable" artifact and not hardcode these (https://github.com/open-telemetry/semantic-conventions-java ???)
    String localServiceName = span.localServiceName();
    if (localServiceName != null) {
      resourceSpansBuilder.getResourceBuilder().addAttributes(KeyValue.newBuilder().setKey("service.name").setValue(AnyValue.newBuilder().setStringValue(localServiceName).build()).build());
    }
    String localIp = span.localIp();
    if (localIp != null) {
      spanBuilder.addAttributes(KeyValue.newBuilder().setKey("net.host.ip").setValue(AnyValue.newBuilder().setStringValue(localIp).build()).build());
    }
    int localPort = span.localPort();
    if (localPort != 0) {
      spanBuilder.addAttributes(KeyValue.newBuilder().setKey("net.host.port").setValue(AnyValue.newBuilder().setIntValue(localPort).build()).build());
    }
    String peerName = span.remoteIp();
    if (peerName != null) {
      spanBuilder.addAttributes(KeyValue.newBuilder().setKey("net.sock.peer.name").setValue(AnyValue.newBuilder().setStringValue(peerName).build()).build());
    }
    String peerIp = span.remoteIp();
    if (peerIp != null) {
      spanBuilder.addAttributes(KeyValue.newBuilder().setKey("net.sock.peer.addr").setValue(AnyValue.newBuilder().setStringValue(peerIp).build()).build());
    }
    int peerPort = span.remotePort();
    if (peerPort != 0) {
      spanBuilder.addAttributes(KeyValue.newBuilder().setKey("net.sock.peer.port").setValue(AnyValue.newBuilder().setIntValue(peerPort).build()).build());
    }
    span.forEachTag(Consumer.INSTANCE, spanBuilder);
    span.forEachAnnotation(Consumer.INSTANCE, spanBuilder);

    scopeSpanBuilder.addSpans(spanBuilder
      .build());
    resourceSpansBuilder.addScopeSpans(scopeSpanBuilder
      .build());
    tracesDataBuilder.addResourceSpans(resourceSpansBuilder.build());
    return tracesDataBuilder.build();
  }

  void maybeAddErrorTag(MutableSpan span) {
    // span.tag(key) iterates: check if we need to first!
    if (span.error() == null) return;
    if (span.tag("error") == null) errorTag.tag(span.error(), null, span);
  }

  @Override public String toString() {
    return delegate.toString();
  }

  /**
   * Overridden to avoid duplicates when added via {@link brave.Tracing.Builder#addSpanHandler(SpanHandler)}
   */
  @Override public final boolean equals(Object o) {
    if (o == this) return true;
    if (!(o instanceof OtlpConvertingSpanReporter)) return false;
    return delegate.equals(((OtlpConvertingSpanReporter) o).delegate);
  }

  /**
   * Overridden to avoid duplicates when added via {@link brave.Tracing.Builder#addSpanHandler(SpanHandler)}
   */
  @Override public final int hashCode() {
    return delegate.hashCode();
  }

  enum Consumer implements TagConsumer<Span.Builder>, AnnotationConsumer<Span.Builder> {
    INSTANCE;

    @Override public void accept(Span.Builder target, String key, String value) {
      target.addAttributesBuilder().setKey(key).setValue(AnyValue.newBuilder().setStringValue(value).build());
    }

    @Override public void accept(Span.Builder target, long timestamp, String value) {
      target.addEventsBuilder().setTimeUnixNano(TimeUnit.MICROSECONDS.toNanos(timestamp)).setName(value);
    }
  }

}
