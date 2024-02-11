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

import java.io.IOException;
import java.util.List;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;
import okhttp3.Call;
import okhttp3.Dispatcher;
import okhttp3.HttpUrl;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okio.Buffer;
import okio.BufferedSink;
import okio.GzipSink;
import okio.Okio;
import zipkin2.reporter.AsyncReporter;
import zipkin2.reporter.BytesMessageSender;
import zipkin2.reporter.CheckResult;
import zipkin2.reporter.ClosedSenderException;
import zipkin2.reporter.Encoding;
import zipkin2.reporter.HttpEndpointSupplier;
import zipkin2.reporter.Sender;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static zipkin2.reporter.Call.propagateIfFatal;
import static zipkin2.reporter.okhttp3.HttpCall.parseResponse;

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
public final class OkHttpSender extends Sender {
  static final Logger logger = Logger.getLogger(OkHttpSender.class.getName());

  /** Creates a sender that posts {@link Encoding#JSON} messages. */
  public static OkHttpSender create(String endpoint) {
    return newBuilder().endpoint(endpoint).build();
  }

  public static Builder newBuilder() {
    return new Builder(new OkHttpClient.Builder());
  }

  public static final class Builder {
    final OkHttpClient.Builder clientBuilder;
    HttpEndpointSupplier.Factory endpointSupplierFactory = HttpEndpointSupplier.CONSTANT_FACTORY;
    String endpoint;
    Encoding encoding = Encoding.JSON;
    boolean compressionEnabled = true;
    int maxRequests = 64;
    int messageMaxBytes = 500_000;

    Builder(OkHttpClient.Builder clientBuilder) {
      this.clientBuilder = clientBuilder;
    }

    Builder(OkHttpSender sender) {
      clientBuilder = sender.client.newBuilder();
      endpointSupplierFactory = sender.endpointSupplierFactory;
      endpoint = sender.endpoint;
      maxRequests = sender.client.dispatcher().getMaxRequests();
      compressionEnabled = sender.compressionEnabled;
      encoding = sender.encoding;
      messageMaxBytes = sender.messageMaxBytes;
    }

    /**
     * No default. See JavaDoc on {@link HttpEndpointSupplier} for implementation notes.
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
      String endpoint = this.endpoint;
      if (endpoint == null) throw new NullPointerException("endpoint == null");

      HttpEndpointSupplier endpointSupplier = endpointSupplierFactory.create(endpoint);
      if (endpointSupplier == null) {
        throw new NullPointerException("endpointSupplierFactory.create() returned null");
      }
      if (endpointSupplier instanceof HttpEndpointSupplier.Constant) {
        endpoint = endpointSupplier.get(); // eagerly resolve the endpoint
        return new OkHttpSender(this, new ConstantHttpUrlSupplier(endpoint));
      }
      return new OkHttpSender(this, new DynamicHttpUrlSupplier(endpointSupplier));
    }
  }

  static HttpUrl toHttpUrl(String endpoint) {
    HttpUrl parsed = HttpUrl.parse(endpoint);
    if (parsed == null) throw new IllegalArgumentException("invalid POST url: " + endpoint);
    return parsed;
  }

  static abstract class HttpUrlSupplier {
    abstract HttpUrl get();

    void close() {
    }
  }

  static final class ConstantHttpUrlSupplier extends HttpUrlSupplier {
    final HttpUrl url;

    ConstantHttpUrlSupplier(String endpoint) {
      this.url = toHttpUrl(endpoint);
    }

    @Override HttpUrl get() {
      return url;
    }

    @Override public String toString() {
      return url.toString();
    }
  }

  static final class DynamicHttpUrlSupplier extends HttpUrlSupplier {

    final HttpEndpointSupplier endpointSupplier;

    DynamicHttpUrlSupplier(HttpEndpointSupplier endpointSupplier) {
      this.endpointSupplier = endpointSupplier;
    }

    @Override HttpUrl get() {
      String endpoint = endpointSupplier.get();
      if (endpoint == null) throw new NullPointerException("endpointSupplier.get() returned null");
      return toHttpUrl(endpoint);
    }

    @Override void close() {
      try {
        endpointSupplier.close();
      } catch (Throwable t) {
        propagateIfFatal(t);
        logger.fine("ignoring error closing endpoint supplier: " + t.getMessage());
      }
    }

    @Override public String toString() {
      return endpointSupplier.toString();
    }
  }

  final HttpEndpointSupplier.Factory endpointSupplierFactory; // for toBuilder()
  final String endpoint; // for toBuilder()

  final HttpUrlSupplier urlSupplier;
  final OkHttpClient client;
  final RequestBodyMessageEncoder encoder;
  final Encoding encoding;
  final int messageMaxBytes, maxRequests;
  final boolean compressionEnabled;

  OkHttpSender(Builder builder, HttpUrlSupplier urlSupplier) {
    endpointSupplierFactory = builder.endpointSupplierFactory;
    endpoint = builder.endpoint;
    this.urlSupplier = urlSupplier;
    encoding = builder.encoding;
    switch (encoding) {
      case JSON:
        encoder = RequestBodyMessageEncoder.JSON;
        break;
      case THRIFT:
        this.encoder = RequestBodyMessageEncoder.THRIFT;
        break;
      case PROTO3:
        encoder = RequestBodyMessageEncoder.PROTO3;
        break;
      default:
        throw new UnsupportedOperationException("Unsupported encoding: " + encoding.name());
    }
    maxRequests = builder.maxRequests;
    messageMaxBytes = builder.messageMaxBytes;
    compressionEnabled = builder.compressionEnabled;
    Dispatcher dispatcher = newDispatcher(maxRequests);

    // doing the extra "build" here prevents us from leaking our dispatcher to the builder
    client = builder.clientBuilder().build().newBuilder().dispatcher(dispatcher).build();
  }

  static Dispatcher newDispatcher(int maxRequests) {
    // bound the executor so that we get consistent performance
    ThreadPoolExecutor dispatchExecutor =
      new ThreadPoolExecutor(0, maxRequests, 60, TimeUnit.SECONDS,
        // Using a synchronous queue means messages will send immediately until we hit max
        // in-flight requests. Once max requests are hit, send will block the caller, which is
        // the AsyncReporter flush thread. This is ok, as the AsyncReporter has a buffer of
        // unsent spans for this purpose.
        new SynchronousQueue<Runnable>(),
        OkHttpSenderThreadFactory.INSTANCE);

    Dispatcher dispatcher = new Dispatcher(dispatchExecutor);
    dispatcher.setMaxRequests(maxRequests);
    dispatcher.setMaxRequestsPerHost(maxRequests);
    return dispatcher;
  }

  enum OkHttpSenderThreadFactory implements ThreadFactory {
    INSTANCE;

    @Override public Thread newThread(Runnable r) {
      return new Thread(r, "OkHttpSender Dispatcher");
    }
  }

  /**
   * Creates a builder out of this object. Note: if the {@link Builder#clientBuilder()} was
   * customized, you'll need to re-apply those customizations.
   */
  public Builder toBuilder() {
    return new Builder(this);
  }

  @Override public int messageSizeInBytes(List<byte[]> encodedSpans) {
    return encoding.listSizeInBytes(encodedSpans);
  }

  @Override public int messageSizeInBytes(int encodedSizeInBytes) {
    return encoding.listSizeInBytes(encodedSizeInBytes);
  }

  @Override public Encoding encoding() {
    return encoding;
  }

  @Override public int messageMaxBytes() {
    return messageMaxBytes;
  }

  /** close is typically called from a different thread */
  final AtomicBoolean closeCalled = new AtomicBoolean();

  /** {@inheritDoc} */
  @Override @Deprecated public zipkin2.reporter.Call<Void> sendSpans(List<byte[]> encodedSpans) {
    if (closeCalled.get()) throw new ClosedSenderException();
    Request request;
    try {
      request = newRequest(encoder.encode(encodedSpans));
    } catch (IOException e) {
      throw Platform.get().uncheckedIOException(e);
    }
    return new HttpCall(client.newCall(request));
  }

  /** Sends spans as a POST to {@link Builder#endpoint(String)}. */
  @Override public void send(List<byte[]> encodedSpans) throws IOException {
    if (closeCalled.get()) throw new ClosedSenderException();
    Request request = newRequest(encoder.encode(encodedSpans));
    Call call = client.newCall(request);
    parseResponse(call.execute());
  }

  /** {@inheritDoc} */
  @Override @Deprecated public CheckResult check() {
    try {
      Request request = new Request.Builder().url(urlSupplier.get())
        .post(RequestBody.create(MediaType.parse("application/json"), "[]")).build();
      try (Response response = client.newCall(request).execute()) {
        if (!response.isSuccessful()) {
          return CheckResult.failed(new RuntimeException("check response failed: " + response));
        }
      }
      return CheckResult.OK;
    } catch (Exception e) {
      return CheckResult.failed(e);
    }
  }

  /** Waits up to a second for in-flight requests to finish before cancelling them */
  @Override public void close() {
    if (!closeCalled.compareAndSet(false, true)) return; // already closed

    urlSupplier.close();

    Dispatcher dispatcher = client.dispatcher();
    dispatcher.executorService().shutdown();
    try {
      if (!dispatcher.executorService().awaitTermination(1, TimeUnit.SECONDS)) {
        dispatcher.cancelAll();
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  Request newRequest(RequestBody body) throws IOException {
    Request.Builder request = new Request.Builder().url(urlSupplier.get());
    // Amplification can occur when the Zipkin endpoint is proxied, and the proxy is instrumented.
    // This prevents that in proxies, such as Envoy, that understand B3 single format,
    request.addHeader("b3", "0");
    if (compressionEnabled) {
      request.addHeader("Content-Encoding", "gzip");
      Buffer gzipped = new Buffer();
      BufferedSink gzipSink = Okio.buffer(new GzipSink(gzipped));
      body.writeTo(gzipSink);
      gzipSink.close();
      body = new BufferRequestBody(body.contentType(), gzipped);
    }
    request.post(body);
    return request.build();
  }

  @Override public String toString() {
    return "OkHttpSender{" + urlSupplier + "}";
  }

  static final class BufferRequestBody extends RequestBody {
    final MediaType contentType;
    final Buffer body;

    BufferRequestBody(MediaType contentType, Buffer body) {
      this.contentType = contentType;
      this.body = body;
    }

    @Override public long contentLength() {
      return body.size();
    }

    @Override public MediaType contentType() {
      return contentType;
    }

    @Override public void writeTo(BufferedSink sink) throws IOException {
      sink.write(body, body.size());
    }
  }
}
