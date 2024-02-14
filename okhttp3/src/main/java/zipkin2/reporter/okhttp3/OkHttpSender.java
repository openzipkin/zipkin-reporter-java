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
package zipkin2.reporter.okhttp3;

import okhttp3.HttpUrl;
import okhttp3.OkHttpClient;
import zipkin2.reporter.AsyncReporter;
import zipkin2.reporter.BytesMessageSender;
import zipkin2.reporter.Encoding;
import zipkin2.reporter.HttpEndpointSupplier;
import zipkin2.reporter.HttpEndpointSuppliers;
import zipkin2.reporter.internal.SenderAdapter;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static zipkin2.reporter.HttpEndpointSuppliers.constantFactory;

/**
 * Reports spans to Zipkin, using its <a href="https://zipkin.io/zipkin-api/#/">POST</a> endpoint.
 *
 * <h3>Usage</h3>
 * <p>
 * This type is designed for {@link AsyncReporter.Builder#builder(BytesMessageSender) the async
 * reporter}.
 *
 * <p>Here's a simple configuration, configured for json:
 *
 * <pre>{@code
 * sender = OkHttpSender.create("http://127.0.0.1:9411/api/v2/spans");
 * }</pre>
 *
 * <p>Here's an example that adds <a href="https://github.com/square/okhttp/blob/master/samples/guide/src/main/java/okhttp3/recipes/Authenticate.java">basic
 * auth</a> (assuming you have an authenticating proxy):
 *
 * <pre>{@code
 * credential = Credentials.basic("me", "secure");
 * sender = OkHttpSender.newBuilder()
 *   .endpoint("https://authenticated-proxy/api/v2/spans")
 *   .clientBuilder().authenticator(new Authenticator() {
 *     @Override
 *     public Request authenticate(Route route, Response response) throws IOException {
 *       if (response.request().header("Authorization") != null) {
 *         return null; // Give up, we've already attempted to authenticate.
 *       }
 *       return response.request().newBuilder()
 *         .header("Authorization", credential)
 *         .build();
 *     }
 *   })
 *   .build();
 * }</pre>
 *
 * <h3>Implementation Notes</h3>
 *
 * <p>This sender is thread-safe.
 */
public final class OkHttpSender extends SenderAdapter {
  /** Creates a sender that posts {@link Encoding#JSON} messages. */
  public static OkHttpSender create(String endpoint) {
    return newBuilder().endpoint(endpoint).build();
  }

  public static Builder newBuilder() {
    return new Builder(new OkHttpClient.Builder());
  }

  public static final class Builder {
    final OkHttpClient.Builder clientBuilder;
    HttpEndpointSupplier.Factory endpointSupplierFactory = constantFactory();
    String endpoint;
    Encoding encoding = Encoding.JSON;
    boolean compressionEnabled = true;
    int maxRequests = 64;
    int messageMaxBytes = 500_000;

    Builder(OkHttpClient.Builder clientBuilder) {
      this.clientBuilder = clientBuilder;
    }

    Builder(OkHttpSender sender) {
      clientBuilder = sender.delegate.client.newBuilder();
      endpointSupplierFactory = sender.endpointSupplierFactory;
      endpoint = sender.endpoint;
      maxRequests = sender.delegate.client.dispatcher().getMaxRequests();
      compressionEnabled = sender.delegate.compressionEnabled;
      encoding = sender.delegate.encoding;
      messageMaxBytes = sender.delegate.messageMaxBytes;
    }

    /**
     * Defaults to {@link HttpEndpointSuppliers#constantFactory()}.
     *
     * <p>See JavaDoc on {@link HttpEndpointSupplier} for implementation notes.
     */
    public Builder endpointSupplierFactory(HttpEndpointSupplier.Factory endpointSupplierFactory) {
      if (endpointSupplierFactory == null) {
        throw new NullPointerException("endpointSupplierFactory == null");
      }
      this.endpointSupplierFactory = endpointSupplierFactory;
      return this;
    }

    /**
     * No default. The POST HttpUrl for zipkin's <a href="https://zipkin.io/zipkin-api/#/">v2 api</a>,
     * usually "http://zipkinhost:9411/api/v2/spans"
     */
    // customizable so that users can re-map /api/v2/spans ex for browser-originated traces
    public Builder endpoint(String endpoint) {
      if (endpoint == null) throw new NullPointerException("endpoint == null");
      this.endpoint = endpoint;
      return this;
    }

    public Builder endpoint(HttpUrl endpoint) {
      if (endpoint == null) throw new NullPointerException("endpoint == null");
      this.endpoint = endpoint.toString();
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

    /** Maximum in-flight requests. Default 64 */
    public Builder maxRequests(int maxRequests) {
      this.maxRequests = maxRequests;
      return this;
    }

    /**
     * Use this to change the encoding used in messages. Default is {@linkplain Encoding#JSON} This
     * also controls the "Content-Type" header when sending spans.
     *
     * <p>Note: If ultimately sending to Zipkin, version 2.8+ is required to process protobuf.
     */
    public Builder encoding(Encoding encoding) {
      if (encoding == null) throw new NullPointerException("encoding == null");
      this.encoding = encoding;
      return this;
    }

    /** Sets the default connect timeout (in milliseconds) for new connections. Default 10000 */
    public Builder connectTimeout(int connectTimeoutMillis) {
      clientBuilder.connectTimeout(connectTimeoutMillis, MILLISECONDS);
      return this;
    }

    /** Sets the default read timeout (in milliseconds) for new connections. Default 10000 */
    public Builder readTimeout(int readTimeoutMillis) {
      clientBuilder.readTimeout(readTimeoutMillis, MILLISECONDS);
      return this;
    }

    /** Sets the default write timeout (in milliseconds) for new connections. Default 10000 */
    public Builder writeTimeout(int writeTimeoutMillis) {
      clientBuilder.writeTimeout(writeTimeoutMillis, MILLISECONDS);
      return this;
    }

    public OkHttpClient.Builder clientBuilder() {
      return clientBuilder;
    }

    public OkHttpSender build() {
      if (endpoint == null) throw new NullPointerException("endpoint == null");
      return new OkHttpSender(this);
    }
  }

  final InternalOkHttpSender delegate;
  final HttpEndpointSupplier.Factory endpointSupplierFactory; // for toBuilder()
  final String endpoint; // for toBuilder()

  OkHttpSender(Builder builder) {
    this.delegate = new InternalOkHttpSender(builder);
    this.endpointSupplierFactory = builder.endpointSupplierFactory; // for toBuilder()
    this.endpoint = builder.endpoint; // for toBuilder()
  }

  /**
   * Creates a builder out of this object. Note: if the {@link Builder#clientBuilder()} was
   * customized, you'll need to re-apply those customizations.
   */
  public Builder toBuilder() {
    return new Builder(this);
  }

  @Override protected BytesMessageSender delegate() {
    return delegate;
  }
}
