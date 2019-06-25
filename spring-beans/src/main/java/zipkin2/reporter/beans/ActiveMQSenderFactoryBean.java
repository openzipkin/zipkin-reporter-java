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

import org.apache.activemq.ActiveMQConnectionFactory;
import org.springframework.beans.factory.config.AbstractFactoryBean;
import zipkin2.codec.Encoding;
import zipkin2.reporter.activemq.ActiveMQSender;

/** Spring XML config does not support chained builders. This converts accordingly */
public class ActiveMQSenderFactoryBean extends AbstractFactoryBean {

  String url, queue, username, password;
  String clientIdPrefix = "zipkin", connectionIdPrefix = "zipkin";
  Encoding encoding;
  Integer messageMaxBytes;

  @Override protected ActiveMQSender createInstance() {
    ActiveMQSender.Builder builder = ActiveMQSender.newBuilder();
    if (url == null) throw new IllegalArgumentException("url is required");
    if (queue != null) builder.queue(queue);

    ActiveMQConnectionFactory connectionFactory;
    if (username != null) {
      connectionFactory = new ActiveMQConnectionFactory(username, password, url);
    } else {
      connectionFactory = new ActiveMQConnectionFactory(url);
    }
    connectionFactory.setClientIDPrefix(clientIdPrefix);
    connectionFactory.setConnectionIDPrefix(connectionIdPrefix);
    builder.connectionFactory(connectionFactory);
    if (encoding != null) builder.encoding(encoding);
    if (messageMaxBytes != null) builder.messageMaxBytes(messageMaxBytes);
    return builder.build();
  }

  @Override public Class<? extends ActiveMQSender> getObjectType() {
    return ActiveMQSender.class;
  }

  @Override public boolean isSingleton() {
    return true;
  }

  @Override protected void destroyInstance(Object instance) {
    ((ActiveMQSender) instance).close();
  }

  public void setUrl(String url) {
    this.url = url;
  }

  public void setQueue(String queue) {
    this.queue = queue;
  }

  public void setClientIdPrefix(String clientIdPrefix) {
    this.clientIdPrefix = clientIdPrefix;
  }

  public String getConnectionIdPrefix() {
    return connectionIdPrefix;
  }

  public void setConnectionIdPrefix(String connectionIdPrefix) {
    this.connectionIdPrefix = connectionIdPrefix;
  }

  public void setUsername(String username) {
    this.username = username;
  }

  public void setPassword(String password) {
    this.password = password;
  }

  public void setEncoding(Encoding encoding) {
    this.encoding = encoding;
  }

  public void setMessageMaxBytes(Integer messageMaxBytes) {
    this.messageMaxBytes = messageMaxBytes;
  }
}
