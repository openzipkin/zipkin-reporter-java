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
import java.util.concurrent.TimeUnit;
import zipkin2.reporter.brave.AsyncZipkinSpanHandler;

/** Spring XML config does not support chained builders. This converts accordingly */
public class AsyncZipkinSpanHandlerFactoryBean extends BaseAsyncFactoryBean {
  Tag<Throwable> errorTag;
  Boolean alwaysReportSpans;

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
    return builder.build();
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
}
