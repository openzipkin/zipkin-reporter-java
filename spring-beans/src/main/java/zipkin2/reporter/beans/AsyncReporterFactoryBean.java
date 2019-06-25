/*
 * Copyright 2016-2019 The OpenZipkin Authors
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
import org.springframework.beans.factory.config.AbstractFactoryBean;
import zipkin2.codec.SpanBytesEncoder;
import zipkin2.reporter.AsyncReporter;
import zipkin2.reporter.ReporterMetrics;
import zipkin2.reporter.Sender;

/** Spring XML config does not support chained builders. This converts accordingly */
public class AsyncReporterFactoryBean extends AbstractFactoryBean {
  Sender sender;
  SpanBytesEncoder encoder;
  ReporterMetrics metrics;
  Integer messageMaxBytes;
  Integer messageTimeout;
  Integer closeTimeout;
  Integer queuedMaxSpans;
  Integer queuedMaxBytes;

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

  @Override public boolean isSingleton() {
    return true;
  }

  public void setSender(Sender sender) {
    this.sender = sender;
  }

  public void setEncoder(SpanBytesEncoder encoder) {
    this.encoder = encoder;
  }

  public void setMetrics(ReporterMetrics metrics) {
    this.metrics = metrics;
  }

  public void setMessageMaxBytes(Integer messageMaxBytes) {
    this.messageMaxBytes = messageMaxBytes;
  }

  public void setMessageTimeout(Integer messageTimeout) {
    this.messageTimeout = messageTimeout;
  }

  public void setCloseTimeout(Integer closeTimeout) {
    this.closeTimeout = closeTimeout;
  }

  public void setQueuedMaxSpans(Integer queuedMaxSpans) {
    this.queuedMaxSpans = queuedMaxSpans;
  }

  public void setQueuedMaxBytes(Integer queuedMaxBytes) {
    this.queuedMaxBytes = queuedMaxBytes;
  }
}
