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
package zipkin2.reporter.beans;

import brave.Tag;
import brave.handler.SpanHandler;
import org.springframework.beans.factory.config.AbstractFactoryBean;
import zipkin2.Span;
import zipkin2.reporter.Reporter;
import zipkin2.reporter.brave.ZipkinSpanHandler;

/** Spring XML config does not support chained builders. This converts accordingly */
public class ZipkinSpanHandlerFactoryBean extends AbstractFactoryBean<SpanHandler> {
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
