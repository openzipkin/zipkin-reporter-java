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

import java.util.concurrent.TimeUnit;
import zipkin2.codec.SpanBytesEncoder;
import zipkin2.reporter.AsyncReporter;

/** Spring XML config does not support chained builders. This converts accordingly */
public class AsyncReporterFactoryBean extends BaseAsyncFactoryBean<AsyncReporter> {
  SpanBytesEncoder encoder;

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

  public void setEncoder(SpanBytesEncoder encoder) {
    this.encoder = encoder;
  }
}
