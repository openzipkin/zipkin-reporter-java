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

import java.io.Closeable;
import java.io.IOException;

import brave.Tag;
import brave.Tags;
import brave.handler.MutableSpan;
import brave.handler.SpanHandler;
import brave.propagation.TraceContext;
import io.opentelemetry.proto.trace.v1.TracesData;
import zipkin2.reporter.Reporter;

/**
 * This allows you to send spans recorded by Brave to a pre-configured {@linkplain Reporter OTLP reporter}.
 *
 * @see brave.Tracing.Builder#addSpanHandler(SpanHandler)
 * @since 2.16.5
 */
public class OtlpSpanHandler extends SpanHandler implements Closeable {

  // SpanHandler not OtlpSpanHandler as it can coerce to NOOP
  public static SpanHandler create(Reporter<TracesData> spanReporter) {
    return newBuilder(spanReporter).build();
  }

  public static Builder newBuilder(Reporter<TracesData> spanReporter) {
    if (spanReporter == null) throw new NullPointerException("spanReporter == null");
    return new ConvertingOtlpSpanHandler.Builder(spanReporter);
  }

  /**
   * Allows this instance to be reconfigured, for example {@link Builder#alwaysReportSpans(boolean)}.
   *
   * <p><em>Note:</em> Call {@link #close()} if you no longer need this instance, as otherwise it
   * can leak resources.
   *
   * @since 2.16.5
   */
  public Builder toBuilder() {
    // For testing, this is easier than making the type abstract: It is package sealed anyway!
    throw new UnsupportedOperationException();
  }

  /**
   * Implementations that throw exceptions on close have bugs. This may result in log warnings,
   * though.
   *
   * @since 2.16.5
   */
  @Override
  public void close() {
    if (this.spanReporter instanceof Closeable) {
      try {
        ((Closeable) this.spanReporter).close();
      }
      catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  public static abstract class Builder {
    Tag<Throwable> errorTag = Tags.ERROR;

    boolean alwaysReportSpans;

    Builder(OtlpSpanHandler otlpSpanHandler) {
      errorTag = otlpSpanHandler.errorTag;
      this.alwaysReportSpans = otlpSpanHandler.alwaysReportSpans;
    }

    Builder() { // sealed
    }

    /**
     * Sets the "error" tag when absent and {@link MutableSpan#error()} is present.
     *
     * <p><em>Note</em>: Zipkin format uses the {@linkplain Tags#ERROR "error" tag}, but
     * alternative formats may have a different tag name or a field entirely. Hence, we only create
     * the "error" tag here, and only if not previously set.
     *
     * @since 2.16.5
     */
    public Builder errorTag(Tag<Throwable> errorTag) {
      if (errorTag == null) throw new NullPointerException("errorTag == null");
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
     * @since 2.16.5
     */
    public Builder alwaysReportSpans(boolean alwaysReportSpans) {
      this.alwaysReportSpans = alwaysReportSpans;
      return this;
    }

    public abstract SpanHandler build();
  }

  final Reporter<MutableSpan> spanReporter;

  final Tag<Throwable> errorTag; // for toBuilder()

  final boolean alwaysReportSpans;

  OtlpSpanHandler(Reporter<MutableSpan> spanReporter, Tag<Throwable> errorTag,
    boolean alwaysReportSpans) {
    this.spanReporter = spanReporter;
    this.errorTag = errorTag;
    this.alwaysReportSpans = alwaysReportSpans;
  }

  @Override
  public boolean end(TraceContext context, MutableSpan span, Cause cause) {
    if (!alwaysReportSpans && !Boolean.TRUE.equals(context.sampled())) return true;
    spanReporter.report(span);
    return true;
  }

  @Override
  public String toString() {
    return spanReporter.toString();
  }

  /**
   * Overridden to avoid duplicates when added via {@link brave.Tracing.Builder#addSpanHandler(SpanHandler)}
   */
  @Override
  public final boolean equals(Object o) {
    if (o == this) return true;
    if (!(o instanceof OtlpSpanHandler)) return false;
    return spanReporter.equals(((OtlpSpanHandler) o).spanReporter);
  }

  /**
   * Overridden to avoid duplicates when added via {@link brave.Tracing.Builder#addSpanHandler(SpanHandler)}
   */
  @Override
  public final int hashCode() {
    return spanReporter.hashCode();
  }
}
