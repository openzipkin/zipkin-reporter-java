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

import brave.ErrorParser;
import brave.handler.FinishedSpanHandler;
import org.springframework.beans.factory.config.AbstractFactoryBean;
import zipkin2.Span;
import zipkin2.reporter.Reporter;
import zipkin2.reporter.brave.ZipkinSpanHandler;

/** Spring XML config does not support chained builders. This converts accordingly */
public class ZipkinSpanHandlerFactoryBean extends AbstractFactoryBean {
  Reporter<Span> spanReporter;
  ErrorParser errorParser;
  Boolean alwaysReportSpans;

  @Override protected FinishedSpanHandler createInstance() {
    ZipkinSpanHandler.Builder builder = ZipkinSpanHandler.newBuilder(spanReporter);
    if (errorParser != null) builder.errorParser(errorParser);
    if (alwaysReportSpans != null) builder.alwaysReportSpans();
    return builder.build();
  }

  @Override protected void destroyInstance(Object instance) {
  }

  @Override public Class<? extends FinishedSpanHandler> getObjectType() {
    return FinishedSpanHandler.class;
  }

  @Override public boolean isSingleton() {
    return true;
  }

  public void setSpanReporter(Reporter<Span> spanReporter) {
    this.spanReporter = spanReporter;
  }

  public void setErrorParser(ErrorParser errorParser) {
    this.errorParser = errorParser;
  }

  public void setAlwaysReportSpans(Boolean alwaysReportSpans) {
    this.alwaysReportSpans = alwaysReportSpans;
  }
}
