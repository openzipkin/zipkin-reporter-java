/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin2.reporter.beans;

import brave.Tag;
import brave.handler.SpanHandler;
import org.springframework.beans.factory.config.AbstractFactoryBean;
import zipkin2.Span;
import zipkin2.reporter.Reporter;
import zipkin2.reporter.brave.ZipkinSpanHandler;

/** Spring XML config does not support chained builders. This converts accordingly */
public class ZipkinSpanHandlerFactoryBean extends AbstractFactoryBean {
  Reporter<Span> spanReporter;
  Tag<Throwable> errorTag;
  Boolean alwaysReportSpans;

  @Override protected SpanHandler createInstance() {
    ZipkinSpanHandler.Builder builder = ZipkinSpanHandler.newBuilder(spanReporter);
    if (errorTag != null) builder.errorTag(errorTag);
    if (alwaysReportSpans != null) builder.alwaysReportSpans(alwaysReportSpans);
    return builder.build();
  }

  @Override public Class<? extends SpanHandler> getObjectType() {
    return SpanHandler.class;
  }

  @Override public boolean isSingleton() {
    return true;
  }

  public void setSpanReporter(Reporter<Span> spanReporter) {
    this.spanReporter = spanReporter;
  }

  public void setErrorTag(Tag<Throwable> errorTag) {
    this.errorTag = errorTag;
  }

  public void setAlwaysReportSpans(Boolean alwaysReportSpans) {
    this.alwaysReportSpans = alwaysReportSpans;
  }
}
