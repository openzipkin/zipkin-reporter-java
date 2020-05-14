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

import brave.Tag;
import brave.handler.MutableSpan;
import brave.handler.SpanHandler;
import java.io.Closeable;
import java.io.Flushable;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import zipkin2.reporter.AsyncReporter;
import zipkin2.reporter.ReporterMetrics;
import zipkin2.reporter.Sender;

/**
 * A {@link brave.handler.SpanHandler} that queues spans on {@link #end} to bundle and send as a
 * bulk <a href="https://zipkin.io/zipkin-api/#/">Zipkin JSON V2</a> message. When the {@link
 * Sender} is HTTP, the endpoint is usually "http://zipkinhost:9411/api/v2/spans".
 *
 * <p>Example:
 * <pre>{@code
 * sender = URLConnectionSender.create("http://localhost:9411/api/v2/spans");
 * zipkinSpanHandler = AsyncZipkinSpanHandler.create(sender); // don't forget to close!
 * tracingBuilder.addSpanHandler(zipkinSpanHandler);
 * }</pre>
 *
 * @see ZipkinSpanHandler if you need to use a different format
 * @see brave.Tracing.Builder#addSpanHandler(SpanHandler)
 * @since 2.14
 */
public final class AsyncZipkinSpanHandler extends ZipkinSpanHandler
    implements Closeable, Flushable {
  /** @since 2.14 */
  public static AsyncZipkinSpanHandler create(Sender sender) {
    return newBuilder(sender).build();
  }

  /** @since 2.14 */
  public static Builder newBuilder(Sender sender) {
    if (sender == null) throw new NullPointerException("sender == null");
    return new Builder(sender);
  }

  /** @since 5.14 */
  public static final class Builder extends ZipkinSpanHandler.Builder {
    final AsyncReporter.Builder delegate;

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
    public AsyncZipkinSpanHandler build() {
      return new AsyncZipkinSpanHandler(delegate.build(new JsonV2Encoder(errorTag)),
          alwaysReportSpans);
    }
  }

  AsyncZipkinSpanHandler(AsyncReporter<MutableSpan> asyncReporter, boolean alwaysReportSpans) {
    super(asyncReporter, alwaysReportSpans);
  }

  @Override public void flush() {
    ((AsyncReporter) spanReporter).flush();
  }

  @Override public void close() {
    ((AsyncReporter) spanReporter).close();
  }
}
