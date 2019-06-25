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
import zipkin2.reporter.libthrift.LibthriftSender;

/** Spring XML config does not support chained builders. This converts accordingly */
public class LibthriftSenderFactoryBean extends AbstractFactoryBean {

  String host;
  Integer connectTimeout, socketTimeout;
  Integer port;
  Integer messageMaxBytes;

  @Override
  protected LibthriftSender createInstance() {
    LibthriftSender.Builder builder = LibthriftSender.newBuilder();
    if (host != null) builder.host(host);
    if (port != null) builder.port(port);
    if (socketTimeout != null) builder.socketTimeout(socketTimeout);
    if (connectTimeout != null) builder.connectTimeout(connectTimeout);
    if (messageMaxBytes != null) builder.messageMaxBytes(messageMaxBytes);
    return builder.build();
  }

  @Override
  public Class<? extends LibthriftSender> getObjectType() {
    return LibthriftSender.class;
  }

  @Override
  public boolean isSingleton() {
    return true;
  }

  @Override
  protected void destroyInstance(Object instance) {
    ((LibthriftSender) instance).close();
  }

  public void setHost(String host) {
    this.host = host;
  }

  public void setPort(Integer port) {
    this.port = port;
  }

  public void setSocketTimeout(Integer socketTimeout) {
    this.socketTimeout = socketTimeout;
  }

  public void setConnectTimeout(Integer connectTimeout) {
    this.connectTimeout = connectTimeout;
  }

  public void setMessageMaxBytes(Integer messageMaxBytes) {
    this.messageMaxBytes = messageMaxBytes;
  }
}
