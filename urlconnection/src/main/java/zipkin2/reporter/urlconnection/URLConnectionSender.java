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
package zipkin2.reporter.urlconnection;

import java.net.URL;
import zipkin2.reporter.BytesMessageSender;
import zipkin2.reporter.ConstantHttpEndpointSupplier;
import zipkin2.reporter.Encoding;
import zipkin2.reporter.HttpEndpointSupplier;
import zipkin2.reporter.HttpEndpointSupplier.Factory;
import zipkin2.reporter.internal.SenderAdapter;

/**
 * Reports spans to Zipkin, using its <a href="https://zipkin.io/zipkin-api/#/">POST</a> endpoint.
 *
 * <p>This sender is thread-safe.
 */
public final class URLConnectionSender extends SenderAdapter {

  /** Creates a sender that posts {@link Encoding#JSON} messages. */
  public static URLConnectionSender create(String endpoint) {
    return newBuilder().endpoint(endpoint).build();
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static final class Builder {
    Factory endpointSupplierFactory = ConstantHttpEndpointSupplier.FACTORY;
    String endpoint;
    Encoding encoding = Encoding.JSON;
    int messageMaxBytes = 500000;
    int connectTimeout = 10 * 1000, readTimeout = 60 * 1000;
    boolean compressionEnabled = true;

    Builder(URLConnectionSender sender) {
      this.endpointSupplierFactory = sender.endpointSupplierFactory;
      this.endpoint = sender.endpoint;
      this.encoding = sender.delegate.encoding();
      this.messageMaxBytes = sender.delegate.messageMaxBytes;
      this.connectTimeout = sender.delegate.connectTimeout;
      this.readTimeout = sender.delegate.readTimeout;
      this.compressionEnabled = sender.delegate.compressionEnabled;
    }

    /**
     * No default. See JavaDoc on {@link HttpEndpointSupplier} for implementation notes.
     */
    public Builder endpointSupplierFactory(Factory endpointSupplierFactory) {
      if (endpointSupplierFactory == null) {
        throw new NullPointerException("endpointSupplierFactory == null");
      }
      this.endpointSupplierFactory = endpointSupplierFactory;
      return this;
    }

    /**
     * No default. The POST URL for zipkin's <a href="https://zipkin.io/zipkin-api/#/">v2 api</a>,
     * usually "http://zipkinhost:9411/api/v2/spans"
     */
    // customizable so that users can re-map /api/v2/spans ex for browser-originated traces
    public Builder endpoint(String endpoint) {
      if (endpoint == null) throw new NullPointerException("endpoint == null");
      this.endpoint = endpoint;
      return this;
    }

    public Builder endpoint(URL endpoint) {
      if (endpoint == null) throw new NullPointerException("endpoint == null");
      this.endpoint = endpoint.toString();
      return this;
    }

    /** Default 10 * 1000 milliseconds. 0 implies no timeout. */
    public Builder connectTimeout(int connectTimeout) {
      this.connectTimeout = connectTimeout;
      return this;
    }

    /** Default 60 * 1000 milliseconds. 0 implies no timeout. */
    public Builder readTimeout(int readTimeout) {
      this.readTimeout = readTimeout;
      return this;
    }

    /** Default true. true implies that spans will be gzipped before transport. */
    public Builder compressionEnabled(boolean compressionEnabled) {
      this.compressionEnabled = compressionEnabled;
      return this;
    }

    /** Maximum size of a message. Default 500KB */
    public Builder messageMaxBytes(int messageMaxBytes) {
      this.messageMaxBytes = messageMaxBytes;
      return this;
    }

    /**
     * Use this to change the encoding used in messages. Default is {@linkplain Encoding#JSON}
     * This also controls the "Content-Type" header when sending spans.
     *
     * <p>Note: If ultimately sending to Zipkin, version 2.8+ is required to process protobuf.
     */
    public Builder encoding(Encoding encoding) {
      if (encoding == null) throw new NullPointerException("encoding == null");
      this.encoding = encoding;
      return this;
    }

    public URLConnectionSender build() {
      if (endpoint == null) throw new NullPointerException("endpoint == null");
      return new URLConnectionSender(this);
    }

    Builder() {
    }
  }

  final InternalURLConnectionSender delegate;
  final Factory endpointSupplierFactory; // for toBuilder()
  final String endpoint; // for toBuilder()

  URLConnectionSender(Builder builder) {
    this.delegate = new InternalURLConnectionSender(builder);
    this.endpointSupplierFactory = builder.endpointSupplierFactory; // for toBuilder()
    this.endpoint = builder.endpoint; // for toBuilder()
  }

  public Builder toBuilder() {
    return new Builder(this);
  }

  @Override protected BytesMessageSender delegate() {
    return delegate;
  }
}
