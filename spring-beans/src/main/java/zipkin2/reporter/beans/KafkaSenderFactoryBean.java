/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin2.reporter.beans;

import org.springframework.beans.factory.config.AbstractFactoryBean;
import zipkin2.reporter.Encoding;
import zipkin2.reporter.kafka.KafkaSender;

/** Spring XML config does not support chained builders. This converts accordingly */
public class KafkaSenderFactoryBean extends AbstractFactoryBean {

  String bootstrapServers, topic;
  Encoding encoding;
  Integer messageMaxBytes;

  @Override protected KafkaSender createInstance() {
    KafkaSender.Builder builder = KafkaSender.newBuilder();
    if (bootstrapServers != null) builder.bootstrapServers(bootstrapServers);
    if (encoding != null) builder.encoding(encoding);
    if (topic != null) builder.topic(topic);
    if (messageMaxBytes != null) builder.messageMaxBytes(messageMaxBytes);
    return builder.build();
  }

  @Override public Class<? extends KafkaSender> getObjectType() {
    return KafkaSender.class;
  }

  @Override public boolean isSingleton() {
    return true;
  }

  @Override protected void destroyInstance(Object instance) {
    ((KafkaSender) instance).close();
  }

  public void setBootstrapServers(String bootstrapServers) {
    this.bootstrapServers = bootstrapServers;
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
