/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin2.reporter.beans;

import java.io.IOException;
import org.springframework.beans.factory.config.AbstractFactoryBean;
import zipkin2.reporter.Encoding;
import zipkin2.reporter.amqp.RabbitMQSender;

/** Spring XML config does not support chained builders. This converts accordingly */
public class RabbitMQSenderFactoryBean extends AbstractFactoryBean {

  String addresses, queue;
  Encoding encoding;
  Integer connectionTimeout;
  String virtualHost;
  String username, password;
  Integer messageMaxBytes;

  @Override protected RabbitMQSender createInstance() {
    RabbitMQSender.Builder builder = RabbitMQSender.newBuilder();
    if (addresses != null) builder.addresses(addresses);
    if (encoding != null) builder.encoding(encoding);
    if (queue != null) builder.queue(queue);
    if (connectionTimeout != null) builder.connectionTimeout(connectionTimeout);
    if (virtualHost != null) builder.virtualHost(virtualHost);
    if (username != null) builder.username(username);
    if (password != null) builder.password(password);
    if (messageMaxBytes != null) builder.messageMaxBytes(messageMaxBytes);
    return builder.build();
  }

  @Override public Class<? extends RabbitMQSender> getObjectType() {
    return RabbitMQSender.class;
  }

  @Override public boolean isSingleton() {
    return true;
  }

  @Override protected void destroyInstance(Object instance) throws IOException {
    ((RabbitMQSender) instance).close();
  }

  public void setAddresses(String addresses) {
    this.addresses = addresses;
  }

  public void setQueue(String queue) {
    this.queue = queue;
  }

  public void setEncoding(Encoding encoding) {
    this.encoding = encoding;
  }

  public void setConnectionTimeout(Integer connectionTimeout) {
    this.connectionTimeout = connectionTimeout;
  }

  public void setVirtualHost(String virtualHost) {
    this.virtualHost = virtualHost;
  }

  public void setUsername(String username) {
    this.username = username;
  }

  public void setPassword(String password) {
    this.password = password;
  }

  public void setMessageMaxBytes(Integer messageMaxBytes) {
    this.messageMaxBytes = messageMaxBytes;
  }
}
