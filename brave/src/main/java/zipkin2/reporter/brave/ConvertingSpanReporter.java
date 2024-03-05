/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin2.reporter.brave;

import brave.Span.Kind;
import brave.Tag;
import brave.handler.MutableSpan;
import brave.handler.MutableSpan.AnnotationConsumer;
import brave.handler.MutableSpan.TagConsumer;
import brave.handler.SpanHandler;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.logging.Logger;
import zipkin2.Endpoint;
import zipkin2.Span;
import zipkin2.reporter.Reporter;

final class ConvertingSpanReporter implements Reporter<MutableSpan> {
  static final Logger logger = Logger.getLogger(ConvertingSpanReporter.class.getName());
  static final Map<Kind, Span.Kind> BRAVE_TO_ZIPKIN_KIND = generateKindMap();

  final Reporter<Span> delegate;
  final Tag<Throwable> errorTag;

  ConvertingSpanReporter(Reporter<Span> delegate, Tag<Throwable> errorTag) {
    this.delegate = delegate;
    this.errorTag = errorTag;
  }

  @Override public void report(MutableSpan span) {
    maybeAddErrorTag(span);
    Span converted = convert(span);
    delegate.report(converted);
  }

  static Span convert(MutableSpan span) {
    Span.Builder result = Span.newBuilder()
        .traceId(span.traceId())
        .parentId(span.parentId())
        .id(span.id())
        .name(span.name());

    long start = span.startTimestamp(), finish = span.finishTimestamp();
    result.timestamp(start);
    if (start != 0 && finish != 0L) result.duration(Math.max(finish - start, 1));

    // use ordinal comparison to defend against version skew
    Kind kind = span.kind();
    if (kind != null) {
      result.kind(BRAVE_TO_ZIPKIN_KIND.get(kind));
    }

    String localServiceName = span.localServiceName(), localIp = span.localIp();
    if (localServiceName != null || localIp != null) {
      result.localEndpoint(Endpoint.newBuilder()
          .serviceName(localServiceName)
          .ip(localIp)
          .port(span.localPort())
          .build());
    }

    String remoteServiceName = span.remoteServiceName(), remoteIp = span.remoteIp();
    if (remoteServiceName != null || remoteIp != null) {
      result.remoteEndpoint(Endpoint.newBuilder()
          .serviceName(remoteServiceName)
          .ip(remoteIp)
          .port(span.remotePort())
          .build());
    }

    span.forEachTag(Consumer.INSTANCE, result);
    span.forEachAnnotation(Consumer.INSTANCE, result);
    if (span.shared()) result.shared(true);
    if (span.debug()) result.debug(true);
    return result.build();
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
    if (!(o instanceof ConvertingSpanReporter)) return false;
    return delegate.equals(((ConvertingSpanReporter) o).delegate);
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
      target.putTag(key, value);
    }

    @Override public void accept(Span.Builder target, long timestamp, String value) {
      target.addAnnotation(timestamp, value);
    }
  }

  /**
   * This keeps the code maintenance free in the rare case there is disparity between Brave and
   * Zipkin kind values.
   */
  static Map<Kind, Span.Kind> generateKindMap() {
    Map<Kind, Span.Kind> result = new LinkedHashMap<Kind, Span.Kind>();
    // Note: Both Brave and Zipkin treat null kind as a local/in-process span
    for (Kind kind : Kind.values()) {
      try {
        result.put(kind, Span.Kind.valueOf(kind.name()));
      } catch (RuntimeException e) {
        logger.warning("Could not map Brave kind " + kind + " to Zipkin");
      }
    }
    return result;
  }
}
