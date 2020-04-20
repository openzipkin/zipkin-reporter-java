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

import brave.ErrorParser;
import brave.handler.FinishedSpanHandler;
import brave.handler.MutableSpan;
import brave.handler.MutableSpan.AnnotationConsumer;
import brave.handler.MutableSpan.TagConsumer;
import brave.propagation.TraceContext;
import zipkin2.Endpoint;
import zipkin2.Span;
import zipkin2.reporter.Reporter;

/**
 * This allows you to send spans recorded by Brave to a {@linkplain Reporter Zipkin reporter}.
 *
 * <p>Ex.
 * <pre>{@code
 * spanReporter = AsyncReporter.create(URLConnectionSender.create("http://localhost:9411/api/v2/spans"));
 * tracingBuilder.addFinishedSpanHandler(ZipkinSpanHandler.create(reporter));
 * }</pre>
 *
 * @see brave.Tracing.Builder#addFinishedSpanHandler(FinishedSpanHandler)
 * @since 2.13
 */
// NOTE: Leave this extending FinishedSpanHandler for a while even after docs update to Brave 5.12's
// addSpanHandler. This allows a floor version of Brave 5.4 and lets more applications migrate who
// may not be able to immediately update Brave at the same time.
public final class ZipkinSpanHandler extends FinishedSpanHandler {
  /** @since 2.13 */
  public static FinishedSpanHandler create(Reporter<Span> spanReporter) {
    return newBuilder(spanReporter).build();
  }

  /** @since 2.13 */
  public static Builder newBuilder(Reporter<Span> spanReporter) {
    return new Builder(spanReporter);
  }

  public static final class Builder {
    Reporter<Span> spanReporter;
    ErrorParser errorParser = new ErrorParser();
    boolean alwaysReportSpans;

    Builder(Reporter<Span> spanReporter) {
      if (spanReporter == null) throw new NullPointerException("spanReporter == null");
      this.spanReporter = spanReporter;
    }

    /**
     * Sets the "error" tag when absent and {@link MutableSpan#error()} is present.
     *
     * @since 2.13
     */
    public Builder errorParser(ErrorParser errorParser) {
      this.errorParser = errorParser;
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
     * <p>This is simpler than a custom {@link FinishedSpanHandler}, because you don't have to
     * duplicate transport mechanics already implemented in the {@link Reporter span reporter}.
     * However, this assumes your backend can properly process the partial traces implied when using
     * conditional sampling. For example, if your sampling condition is not consistent on a call
     * tree, the resulting data could appear broken.
     *
     * @see TraceContext#sampledLocal()
     * @since 2.13
     */
    public Builder alwaysReportSpans() {
      this.alwaysReportSpans = true;
      return this;
    }

    public FinishedSpanHandler build() {
      if (spanReporter == Reporter.NOOP) return FinishedSpanHandler.NOOP;
      return new ZipkinSpanHandler(this);
    }
  }

  final Reporter<Span> spanReporter;
  final ErrorParser errorParser;
  final boolean alwaysReportSpans;

  ZipkinSpanHandler(Builder builder) {
    this.spanReporter = builder.spanReporter;
    this.errorParser = builder.errorParser;
    this.alwaysReportSpans = builder.alwaysReportSpans;
  }

  @Override public boolean handle(TraceContext context, MutableSpan span) {
    if (!alwaysReportSpans && !Boolean.TRUE.equals(context.sampled())) return true;
    maybeAddErrorTag(span);
    Span converted = convert(context, span);
    spanReporter.report(converted);
    return true;
  }

  // Do not use @Override annotation to ensure compatibility with Brave 5.6
  public boolean supportsOrphans() {
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
    brave.Span.Kind kind = span.kind();
    if (kind != null && kind.ordinal() < Span.Kind.values().length) {
      result.kind(Span.Kind.values()[kind.ordinal()]);
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

  void maybeAddErrorTag(MutableSpan span) {
    // span.tag(key) iterates: check if we need to first!
    if (span.error() == null) return;
    if (span.tag("error") == null) errorParser.error(span.error(), span);
  }

  @Override public String toString() {
    return spanReporter.toString();
  }

  /**
   * Overridden to avoid duplicates when added via {@link brave.Tracing.Builder#addFinishedSpanHandler(FinishedSpanHandler)}
   */
  @Override public final boolean equals(Object o) {
    if (o == this) return true;
    if (!(o instanceof ZipkinSpanHandler)) return false;
    return spanReporter.equals(((ZipkinSpanHandler) o).spanReporter);
  }

  /**
   * Overridden to avoid duplicates when added via {@link brave.Tracing.Builder#addFinishedSpanHandler(FinishedSpanHandler)}
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
}
