/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin2.reporter.beans;

import io.opentelemetry.api.internal.StringUtils;
import org.springframework.beans.factory.config.AbstractFactoryBean;
import zipkin2.reporter.Encoding;
import zipkin2.reporter.pulsar.PulsarSender;

import java.io.IOException;

/** Spring XML config does not support chained builders. This converts accordingly */
public class PulsarSenderFactoryBean extends AbstractFactoryBean {

  String serviceUrl, topic;
  Encoding encoding;
  Integer messageMaxBytes;

  @Override protected PulsarSender createInstance() {
    PulsarSender.Builder builder = PulsarSender.newBuilder();
    if (serviceUrl != null) builder.serviceUrl(serviceUrl);
    if (topic != null) builder.topic(topic);
    if (encoding != null) builder.encoding(encoding);
    if (messageMaxBytes != null) builder.messageMaxBytes(messageMaxBytes);
    return builder.build();
  }

  @Override public Class<? extends PulsarSender> getObjectType() {
    return PulsarSender.class;
  }

  @Override public boolean isSingleton() {
    return true;
  }

  @Override protected void destroyInstance(Object instance) throws IOException {
    ((PulsarSender) instance).close();
  }

  public void setServiceUrl(String serviceUrl) {
    this.serviceUrl = serviceUrl;
  }

  public void setTopic(String topic) {
    this.topic = topic;
  }

  public void setEncoding(Encoding encoding) {
    this.encoding = encoding;
  }

  public void setMessageMaxBytes(Integer messageMaxBytes) {
    this.messageMaxBytes = messageMaxBytes;
  }
}
