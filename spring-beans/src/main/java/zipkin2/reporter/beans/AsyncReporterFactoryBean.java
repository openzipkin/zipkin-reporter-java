/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin2.reporter.beans;

import java.util.concurrent.TimeUnit;
import zipkin2.Span;
import zipkin2.reporter.AsyncReporter;
import zipkin2.reporter.BytesEncoder;
import zipkin2.reporter.Encoding;
import zipkin2.reporter.SpanBytesEncoder;

/** Spring XML config does not support chained builders. This converts accordingly */
public class AsyncReporterFactoryBean extends BaseAsyncFactoryBean {
  BytesEncoder<Span> encoder;

  @Override public Class<? extends AsyncReporter> getObjectType() {
    return AsyncReporter.class;
  }

  @Override protected AsyncReporter createInstance() {
    AsyncReporter.Builder builder = AsyncReporter.builder(sender);
    if (metrics != null) builder.metrics(metrics);
    if (messageMaxBytes != null) builder.messageMaxBytes(messageMaxBytes);
    if (messageTimeout != null) builder.messageTimeout(messageTimeout, TimeUnit.MILLISECONDS);
    if (closeTimeout != null) builder.closeTimeout(closeTimeout, TimeUnit.MILLISECONDS);
    if (queuedMaxSpans != null) builder.queuedMaxSpans(queuedMaxSpans);
    if (queuedMaxBytes != null) builder.queuedMaxBytes(queuedMaxBytes);
    return encoder != null ? builder.build(encoder) : builder.build();
  }

  @Override protected void destroyInstance(Object instance) {
    ((AsyncReporter) instance).close();
  }

  // Object to allow built-in encoder types.
  public void setEncoder(Object encoder) {
    if (encoder instanceof String) {
      this.encoder = SpanBytesEncoder.forEncoding(Encoding.valueOf(encoder.toString()));
    } else if (encoder instanceof BytesEncoder) {
      this.encoder = (BytesEncoder) encoder;
    }
  }
}
