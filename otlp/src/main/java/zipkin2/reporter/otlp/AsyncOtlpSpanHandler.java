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
import java.io.Flushable;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import brave.Tag;
import brave.handler.SpanHandler;
import zipkin2.reporter.AsyncReporter;
import zipkin2.reporter.ReporterMetrics;
import zipkin2.reporter.Sender;
import zipkin2.reporter.internal.InternalReporter;

/**
 * A {@link SpanHandler} that queues spans on {@link #end} to bundle and send as a PROTO message. When the {@link
 * Sender} is HTTP, the endpoint is usually "http://otlphost:4318/v1/traces".
 *
 * <p>Example:
 * <pre>{@code
 * sender = URLConnectionSender.create("http://localhost:4318/v1/spans");
 * spanHandler = AsyncOtlpSpanHandler.create(sender); // don't forget to close!
 * tracingBuilder.addSpanHandler(spanHandler);
 * }</pre>
 *
 * @see OtlpSpanHandler
 * @see brave.Tracing.Builder#addSpanHandler(SpanHandler)
 * @since 2.16
 */
public final class AsyncOtlpSpanHandler extends OtlpSpanHandler
    implements Closeable, Flushable {
  /** @since 2.14 */
  public static AsyncOtlpSpanHandler create(Sender sender) {
    return newBuilder(sender).build();
  }

  /** @since 2.14 */
  public static Builder newBuilder(Sender sender) {
    if (sender == null) throw new NullPointerException("sender == null");
    return new Builder(sender);
  }

  @Override public Builder toBuilder() {
    return new Builder(this);
  }

  /** @since 2.14 */
  public static final class Builder extends OtlpSpanHandler.Builder {
    final AsyncReporter.Builder delegate;

    Builder(AsyncOtlpSpanHandler spanHandler) {
      super(spanHandler);
      delegate = InternalReporter.instance.toBuilder(
          (AsyncReporter<?>) spanHandler.spanReporter);
    }

    Builder(Sender sender) {
      this.delegate = AsyncReporter.builder(sender);
    }

    /**
     * @see AsyncReporter.Builder#threadFactory(ThreadFactory)
     * @since 2.14
     */
    public Builder threadFactory(ThreadFactory threadFactory) {
      delegate.threadFactory(threadFactory);
      return this;
    }

    /**
     * @see AsyncReporter.Builder#metrics(ReporterMetrics)
     * @since 2.14
     */
    public Builder metrics(ReporterMetrics metrics) {
      delegate.metrics(metrics);
      return this;
    }

    /**
     * @see AsyncReporter.Builder#messageMaxBytes(int)
     * @since 2.14
     */
    public Builder messageMaxBytes(int messageMaxBytes) {
      delegate.messageMaxBytes(messageMaxBytes);
      return this;
    }

    /**
     * @see AsyncReporter.Builder#messageTimeout(long, TimeUnit)
     * @since 2.14
     */
    public Builder messageTimeout(long timeout, TimeUnit unit) {
      delegate.messageTimeout(timeout, unit);
      return this;
    }

    /**
     * @see AsyncReporter.Builder#closeTimeout(long, TimeUnit)
     * @since 2.14
     */
    public Builder closeTimeout(long timeout, TimeUnit unit) {
      delegate.closeTimeout(timeout, unit);
      return this;
    }

    /**
     * @see AsyncReporter.Builder#queuedMaxSpans(int)
     * @since 2.14
     */
    public Builder queuedMaxSpans(int queuedMaxSpans) {
      delegate.queuedMaxSpans(queuedMaxSpans);
      return this;
    }

    /**
     * @see AsyncReporter.Builder#queuedMaxBytes(int)
     * @since 2.14
     */
    public Builder queuedMaxBytes(int queuedMaxBytes) {
      delegate.queuedMaxBytes(queuedMaxBytes);
      return this;
    }

    @Override public Builder errorTag(Tag<Throwable> errorTag) {
      return (Builder) super.errorTag(errorTag);
    }

    @Override public Builder alwaysReportSpans(boolean alwaysReportSpans) {
      return (Builder) super.alwaysReportSpans(alwaysReportSpans);
    }

    // AsyncZipkinSpanHandler not SpanHandler, so that Flushable and Closeable are accessible
    public AsyncOtlpSpanHandler build() {
      return new AsyncOtlpSpanHandler(this);
    }
  }

  AsyncOtlpSpanHandler(Builder builder) {
    super(builder.delegate.build(Proto3Encoder.INSTANCE),
        builder.errorTag, builder.alwaysReportSpans);
  }

  @Override public void flush() {
    ((AsyncReporter) spanReporter).flush();
  }

  @Override public void close() {
    ((AsyncReporter) spanReporter).close();
  }
}
