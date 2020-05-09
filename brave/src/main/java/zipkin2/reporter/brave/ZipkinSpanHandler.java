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

import brave.Span.Kind;
import brave.Tag;
import brave.Tags;
import brave.handler.MutableSpan;
import brave.handler.MutableSpan.AnnotationConsumer;
import brave.handler.MutableSpan.TagConsumer;
import brave.handler.SpanHandler;
import brave.propagation.TraceContext;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.logging.Logger;
import zipkin2.Endpoint;
import zipkin2.Span;
import zipkin2.reporter.Reporter;

/**
 * This allows you to send spans recorded by Brave to a {@linkplain Reporter Zipkin reporter}.
 *
 * <p>Ex.
 * <pre>{@code
 * spanReporter = AsyncReporter.create(URLConnectionSender.create("http://localhost:9411/api/v2/spans"));
 * tracingBuilder.addSpanHandler(ZipkinSpanHandler.create(reporter));
 * }</pre>
 *
 * @see brave.Tracing.Builder#addSpanHandler(SpanHandler)
 * @since 2.13
 */
public final class ZipkinSpanHandler extends SpanHandler {
  static final Logger logger = Logger.getLogger(ZipkinSpanHandler.class.getName());
  static final Map<Kind, Span.Kind> BRAVE_TO_ZIPKIN_KIND = generateKindMap();

  /** @since 2.13 */
  public static SpanHandler create(Reporter<Span> spanReporter) {
    return newBuilder(spanReporter).build();
  }

  /** @since 2.13 */
  public static Builder newBuilder(Reporter<Span> spanReporter) {
    return new Builder(spanReporter);
  }

  public static final class Builder {
    Reporter<Span> spanReporter;
    Tag<Throwable> errorTag = Tags.ERROR;
    boolean alwaysReportSpans;

    Builder(Reporter<Span> spanReporter) {
      if (spanReporter == null) throw new NullPointerException("spanReporter == null");
      this.spanReporter = spanReporter;
    }

    /**
     * Sets the "error" tag when absent and {@link MutableSpan#error()} is present.
     *
     * <p><em>Note</em>: Zipkin format uses the {@linkplain Tags#ERROR "error" tag}, but
     * alternative formats may have a different tag name or a field entirely. Hence, we only create the "error"
     * tag here, and only if not previously set.
     *
     * @since 2.13
     */
    public Builder errorTag(Tag<Throwable> errorTag) {
      this.errorTag = errorTag;
      return this;
    }

    /**
     * When true, all spans {@link TraceContext#sampledLocal() sampled locally} are reported to the
     * span reporter, even if they aren't sampled remotely. Defaults to {@code false}.
     *
     * <p>The primary use case is to implement a <a href="https://github.com/openzipkin-contrib/zipkin-secondary-sampling">sampling
     * overlay</a>, such as boosting the sample rate for a subset of the network depending on the
     * value of a baggage field. This means that data will report when either the trace is normally
     * sampled, or secondarily sampled via a custom header.
     *
     * <p>This is simpler than a custom {@link SpanHandler}, because you don't have to
     * duplicate transport mechanics already implemented in the {@link Reporter span reporter}.
     * However, this assumes your backend can properly process the partial traces implied when using
     * conditional sampling. For example, if your sampling condition is not consistent on a call
     * tree, the resulting data could appear broken.
     *
     * @see TraceContext#sampledLocal()
     * @since 2.13
     */
    public Builder alwaysReportSpans(boolean alwaysReportSpans) {
      this.alwaysReportSpans = alwaysReportSpans;
      return this;
    }

    public SpanHandler build() {
      if (spanReporter == Reporter.NOOP) return SpanHandler.NOOP;
      return new ZipkinSpanHandler(this);
    }
  }

  final Reporter<Span> spanReporter;
  final Tag<Throwable> errorTag;
  final boolean alwaysReportSpans;

  ZipkinSpanHandler(Builder builder) {
    this.spanReporter = builder.spanReporter;
    this.errorTag = builder.errorTag;
    this.alwaysReportSpans = builder.alwaysReportSpans;
  }

  @Override public boolean end(TraceContext context, MutableSpan span, Cause cause) {
    // Abandon explicitly means don't send to the tracing system! This state is might be used for
    // tracking functions, so return true to retain vs false.
    if (cause == Cause.ABANDONED) return true;

    if (!alwaysReportSpans && !Boolean.TRUE.equals(context.sampled())) return true;
    maybeAddErrorTag(context, span);
    Span converted = convert(context, span);
    spanReporter.report(converted);
    return true;
  }

  static Span convert(TraceContext context, MutableSpan span) {
    Span.Builder result = Span.newBuilder()
        .traceId(context.traceIdString())
        .parentId(context.parentIdString())
        .id(context.spanId())
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
    if (context.debug()) result.debug(true);
    return result.build();
  }

  void maybeAddErrorTag(TraceContext context, MutableSpan span) {
    // span.tag(key) iterates: check if we need to first!
    if (span.error() == null) return;
    if (span.tag("error") == null) errorTag.tag(span.error(), context, span);
  }

  @Override public String toString() {
    return spanReporter.toString();
  }

  /**
   * Overridden to avoid duplicates when added via {@link brave.Tracing.Builder#addSpanHandler(SpanHandler)}
   */
  @Override public final boolean equals(Object o) {
    if (o == this) return true;
    if (!(o instanceof ZipkinSpanHandler)) return false;
    return spanReporter.equals(((ZipkinSpanHandler) o).spanReporter);
  }

  /**
   * Overridden to avoid duplicates when added via {@link brave.Tracing.Builder#addSpanHandler(SpanHandler)}
   */
  @Override public final int hashCode() {
    return spanReporter.hashCode();
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
    Map<Kind, Span.Kind> result = new LinkedHashMap<>();
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
