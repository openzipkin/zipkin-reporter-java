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
package zipkin2.reporter;

import java.io.Closeable;
import java.io.Flushable;
import java.util.List;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/**
 * As spans are reported, they are encoded and added to a pending queue. The task of sending spans
 * happens on a separate thread which calls {@link #flush()}. By doing so, callers are protected
 * from latency or exceptions possible when exporting spans out of process.
 *
 * <p>Spans are bundled into messages based on size in bytes or a timeout, whichever happens first.
 *
 * <p>The thread that sends flushes spans to the {@linkplain BytesMessageSender} does so in a synchronous loop.
 * This means that even asynchronous transports will wait for an ack before sending a next message.
 * We do this so that a surge of spans doesn't overrun memory or bandwidth via hundreds or
 * thousands of in-flight messages. The downside of this is that reporting is limited in speed to
 * what a single thread can clear. When a thread cannot clear the backlog, new spans are dropped.
 *
 * @param <S> type of the span, usually {@link zipkin2.Span}
 */
// This is effectively, but not explicitly final as it was not final in version 2.x.
public class AsyncReporter<S> extends Component implements Reporter<S>, Closeable, Flushable {

  /**
   * Builds a json reporter for <a href="https://zipkin.io/zipkin-api/#/">Zipkin V2</a>. If http,
   * the endpoint of the sender is usually "http://zipkinhost:9411/api/v2/spans".
   *
   * <p>After a certain threshold, spans are drained and {@link BytesMessageSender#send(List) sent}
   * to Zipkin collectors.
   */
  public static AsyncReporter<zipkin2.Span> create(BytesMessageSender sender) {
    return new Builder(sender).build();
  }

  /** Like {@link #create(BytesMessageSender)}, except you can configure settings such as the timeout. */
  public static Builder builder(BytesMessageSender sender) {
    return new Builder(sender);
  }

  final zipkin2.reporter.internal.AsyncReporter<S> delegate;

  AsyncReporter(zipkin2.reporter.internal.AsyncReporter<S> delegate) {
    this.delegate = delegate;
  }

  @Override public void report(S span) {
    delegate.report(span);
  }

  /**
   * Calling this will flush any pending spans to the transport on the current thread.
   *
   * <p>Note: If you set {@link Builder#messageTimeout(long, TimeUnit) message timeout} to zero, you
   * must call this externally as otherwise spans will never be sent.
   *
   * @throws IllegalStateException if closed
   */
  @Override public void flush() {
    delegate.flush();
  }

  /** Shuts down the sender thread, and increments drop metrics if there were any unsent spans. */
  @Override public void close() {
    delegate.close();
  }

  @Override public String toString() {
    return delegate.toString();
  }

  public static final class Builder {
    final zipkin2.reporter.internal.AsyncReporter.Builder delegate;
    final Encoding encoding;

    Builder(BytesMessageSender sender) {
      this.delegate = zipkin2.reporter.internal.AsyncReporter.newBuilder(sender);
      this.encoding = sender.encoding();
    }

    /**
     * Launches the flush thread when {@link #messageTimeout} is greater than zero.
     */
    public Builder threadFactory(ThreadFactory threadFactory) {
      this.delegate.threadFactory(threadFactory);
      return this;
    }

    /**
     * Aggregates and reports reporter metrics to a monitoring system. Defaults to no-op.
     */
    public Builder metrics(ReporterMetrics metrics) {
      this.delegate.metrics(metrics);
      return this;
    }

    /**
     * Maximum bytes sendable per message including overhead. Defaults to, and is limited by {@link
     * BytesMessageSender#messageMaxBytes()}.
     */
    public Builder messageMaxBytes(int messageMaxBytes) {
      this.delegate.messageMaxBytes(messageMaxBytes);
      return this;
    }

    /**
     * Default 1 second. 0 implies spans are {@link #flush() flushed} externally.
     *
     * <p>Instead of sending one message at a time, spans are bundled into messages, up to {@link
     * BytesMessageSender#messageMaxBytes()}. This timeout ensures that spans are not stuck in an
     * incomplete message.
     *
     * <p>Note: this timeout starts when the first unsent span is reported.
     */
    public Builder messageTimeout(long timeout, TimeUnit unit) {
      this.delegate.messageTimeout(timeout, unit);
      return this;
    }

    /** How long to block for in-flight spans to send out-of-process on close. Default 1 second */
    public Builder closeTimeout(long timeout, TimeUnit unit) {
      this.delegate.closeTimeout(timeout, unit);
      return this;
    }

    /** Maximum backlog of spans reported vs sent. Default 10000 */
    public Builder queuedMaxSpans(int queuedMaxSpans) {
      this.delegate.queuedMaxSpans(queuedMaxSpans);
      return this;
    }

    /** Maximum backlog of span bytes reported vs sent. Default 1% of heap */
    public Builder queuedMaxBytes(int queuedMaxBytes) {
      this.delegate.queuedMaxBytes(queuedMaxBytes);
      return this;
    }

    /** Builds an async reporter that encodes zipkin spans as they are reported. */
    public AsyncReporter<zipkin2.Span> build() {
      switch (encoding) {
        case JSON:
          return build(SpanBytesEncoder.JSON_V2);
        case PROTO3:
          return build(SpanBytesEncoder.PROTO3);
        case THRIFT:
          return build(SpanBytesEncoder.THRIFT);
        default:
          throw new UnsupportedOperationException(encoding.name());
      }
    }

    /** Builds an async reporter that encodes arbitrary spans as they are reported. */
    public <S> AsyncReporter<S> build(BytesEncoder<S> encoder) {
      if (encoder == null) throw new NullPointerException("encoder == null");
      return new AsyncReporter<S>(delegate.build(new BytesEncoderAdapter<S>(encoder)));
    }
  }

  static final class BytesEncoderAdapter<S> implements BytesEncoder<S> {
    final BytesEncoder<S> delegate;

    BytesEncoderAdapter(BytesEncoder<S> delegate) {
      this.delegate = delegate;
    }

    @Override public Encoding encoding() {
      return delegate.encoding();
    }

    @Override public int sizeInBytes(S input) {
      return delegate.sizeInBytes(input);
    }

    @Override public byte[] encode(S input) {
      return delegate.encode(input);
    }

    @Override public String toString() {
      return delegate.toString();
    }
  }
}
