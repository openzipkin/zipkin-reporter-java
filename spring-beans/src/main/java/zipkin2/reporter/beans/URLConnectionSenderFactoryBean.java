/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin2.reporter.beans;

import org.springframework.beans.factory.config.AbstractFactoryBean;
import zipkin2.reporter.Encoding;
import zipkin2.reporter.HttpEndpointSupplier;
import zipkin2.reporter.urlconnection.URLConnectionSender;

/** Spring XML config does not support chained builders. This converts accordingly */
public class URLConnectionSenderFactoryBean extends AbstractFactoryBean {
  HttpEndpointSupplier.Factory endpointSupplierFactory;
  String endpoint;
  Encoding encoding;
  Integer connectTimeout, readTimeout;
  Boolean compressionEnabled;
  Integer messageMaxBytes;

  @Override protected URLConnectionSender createInstance() {
    URLConnectionSender.Builder builder = URLConnectionSender.newBuilder();
    if (endpointSupplierFactory != null) builder.endpointSupplierFactory(endpointSupplierFactory);
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

  public void setEndpointSupplierFactory(HttpEndpointSupplier.Factory endpointSupplierFactory) {
    this.endpointSupplierFactory = endpointSupplierFactory;
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
