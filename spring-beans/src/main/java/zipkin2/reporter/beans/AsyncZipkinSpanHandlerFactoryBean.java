/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin2.reporter.beans;

import brave.Tag;
import brave.handler.MutableSpan;
import java.util.concurrent.TimeUnit;
import zipkin2.reporter.BytesEncoder;
import zipkin2.reporter.Encoding;
import zipkin2.reporter.brave.AsyncZipkinSpanHandler;
import zipkin2.reporter.brave.MutableSpanBytesEncoder;

/** Spring XML config does not support chained builders. This converts accordingly */
public class AsyncZipkinSpanHandlerFactoryBean extends BaseAsyncFactoryBean {
  Tag<Throwable> errorTag;
  Boolean alwaysReportSpans;

  BytesEncoder<MutableSpan> encoder;

  @Override public Class<? extends AsyncZipkinSpanHandler> getObjectType() {
    return AsyncZipkinSpanHandler.class;
  }

  @Override protected AsyncZipkinSpanHandler createInstance() {
    AsyncZipkinSpanHandler.Builder builder = AsyncZipkinSpanHandler.newBuilder(sender);
    if (errorTag != null) builder.errorTag(errorTag);
    if (alwaysReportSpans != null) builder.alwaysReportSpans(alwaysReportSpans);
    if (metrics != null) builder.metrics(metrics);
    if (messageMaxBytes != null) builder.messageMaxBytes(messageMaxBytes);
    if (messageTimeout != null) builder.messageTimeout(messageTimeout, TimeUnit.MILLISECONDS);
    if (closeTimeout != null) builder.closeTimeout(closeTimeout, TimeUnit.MILLISECONDS);
    if (queuedMaxSpans != null) builder.queuedMaxSpans(queuedMaxSpans);
    if (queuedMaxBytes != null) builder.queuedMaxBytes(queuedMaxBytes);
    return encoder != null ? builder.build(encoder) : builder.build();
  }

  @Override protected void destroyInstance(Object instance) {
    ((AsyncZipkinSpanHandler) instance).close();
  }

  public void setErrorTag(Tag<Throwable> errorTag) {
    this.errorTag = errorTag;
  }

  public void setAlwaysReportSpans(Boolean alwaysReportSpans) {
    this.alwaysReportSpans = alwaysReportSpans;
  }

  // Object to allow built-in encoder types.
  public void setEncoder(Object encoder) {
    if (encoder instanceof String) {
      this.encoder = MutableSpanBytesEncoder.forEncoding(Encoding.valueOf(encoder.toString()));
    } else if (encoder instanceof BytesEncoder) {
      this.encoder = (BytesEncoder) encoder;
    }
  }
}
