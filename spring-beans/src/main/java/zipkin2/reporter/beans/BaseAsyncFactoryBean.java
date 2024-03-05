/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin2.reporter.beans;

import org.springframework.beans.factory.config.AbstractFactoryBean;
import zipkin2.reporter.BytesMessageSender;
import zipkin2.reporter.ReporterMetrics;

abstract class BaseAsyncFactoryBean extends AbstractFactoryBean {
  BytesMessageSender sender;
  ReporterMetrics metrics;
  Integer messageMaxBytes;
  Integer messageTimeout;
  Integer closeTimeout;
  Integer queuedMaxSpans;
  Integer queuedMaxBytes;

  @Override public boolean isSingleton() {
    return true;
  }

  public void setSender(BytesMessageSender sender) {
    this.sender = sender;
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
