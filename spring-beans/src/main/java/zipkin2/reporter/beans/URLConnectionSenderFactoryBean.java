/*
 * Copyright 2016-2024 The OpenZipkin Authors
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
import zipkin2.reporter.Encoding;
import zipkin2.reporter.urlconnection.URLConnectionSender;

/** Spring XML config does not support chained builders. This converts accordingly */
public class URLConnectionSenderFactoryBean extends AbstractFactoryBean {

  String endpoint;
  Encoding encoding;
  Integer connectTimeout, readTimeout;
  Boolean compressionEnabled;
  Integer messageMaxBytes;

  @Override protected URLConnectionSender createInstance() {
    URLConnectionSender.Builder builder = URLConnectionSender.newBuilder();
    if (endpoint != null) builder.endpoint(endpoint);
    if (encoding != null) builder.encoding(encoding);
    if (connectTimeout != null) builder.connectTimeout(connectTimeout);
    if (readTimeout != null) builder.readTimeout(readTimeout);
    if (compressionEnabled != null) builder.compressionEnabled(compressionEnabled);
    if (messageMaxBytes != null) builder.messageMaxBytes(messageMaxBytes);
    return builder.build();
  }

  @Override public Class<? extends URLConnectionSender> getObjectType() {
    return URLConnectionSender.class;
  }

  @Override public boolean isSingleton() {
    return true;
  }

  @Override protected void destroyInstance(Object instance) {
    ((URLConnectionSender) instance).close();
  }

  public void setEndpoint(String endpoint) {
    this.endpoint = endpoint;
  }

  public void setEncoding(Encoding encoding) {
    this.encoding = encoding;
  }

  public void setConnectTimeout(Integer connectTimeout) {
    this.connectTimeout = connectTimeout;
  }

  public void setReadTimeout(Integer readTimeout) {
    this.readTimeout = readTimeout;
  }

  public void setCompressionEnabled(Boolean compressionEnabled) {
    this.compressionEnabled = compressionEnabled;
  }

  public void setMessageMaxBytes(Integer messageMaxBytes) {
    this.messageMaxBytes = messageMaxBytes;
  }
}
