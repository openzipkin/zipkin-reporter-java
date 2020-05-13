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

import org.springframework.beans.factory.config.AbstractFactoryBean;
import zipkin2.reporter.ReporterMetrics;
import zipkin2.reporter.Sender;

abstract class BaseAsyncFactoryBean extends AbstractFactoryBean {
  Sender sender;
  ReporterMetrics metrics;
  Integer messageMaxBytes;
  Integer messageTimeout;
  Integer closeTimeout;
  Integer queuedMaxSpans;
  Integer queuedMaxBytes;

  @Override public boolean isSingleton() {
    return true;
  }

  public void setSender(Sender sender) {
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
