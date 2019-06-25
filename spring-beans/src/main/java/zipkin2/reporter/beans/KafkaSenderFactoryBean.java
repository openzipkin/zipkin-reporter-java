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

import org.springframework.beans.factory.config.AbstractFactoryBean;
import zipkin2.codec.Encoding;
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
