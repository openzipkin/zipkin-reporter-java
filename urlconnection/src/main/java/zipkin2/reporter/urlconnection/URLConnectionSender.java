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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;
import java.util.zip.GZIPOutputStream;
import zipkin2.reporter.BytesMessageEncoder;
import zipkin2.reporter.Call;
import zipkin2.reporter.Callback;
import zipkin2.reporter.CheckResult;
import zipkin2.reporter.ClosedSenderException;
import zipkin2.reporter.Encoding;
import zipkin2.reporter.HttpEndpointSupplier;
import zipkin2.reporter.Sender;

import static zipkin2.reporter.Call.propagateIfFatal;

/**
 * Reports spans to Zipkin, using its <a href="https://zipkin.io/zipkin-api/#/">POST</a> endpoint.
 *
 * <p>This sender is thread-safe.
 */
public final class URLConnectionSender extends Sender {
  static final Logger logger = Logger.getLogger(URLConnectionSender.class.getName());

  /** Creates a sender that posts {@link Encoding#JSON} messages. */
  public static URLConnectionSender create(String endpoint) {
    return newBuilder().endpoint(endpoint).build();
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static final class Builder {
    HttpEndpointSupplier.Factory endpointSupplierFactory = HttpEndpointSupplier.CONSTANT_FACTORY;
    String endpoint;
    Encoding encoding = Encoding.JSON;
    int messageMaxBytes = 500000;
    int connectTimeout = 10 * 1000, readTimeout = 60 * 1000;
    boolean compressionEnabled = true;

    Builder(URLConnectionSender sender) {
      this.endpointSupplierFactory = sender.endpointSupplierFactory;
      this.endpoint = sender.endpoint;
      this.encoding = sender.encoding;
      this.messageMaxBytes = sender.messageMaxBytes;
      this.connectTimeout = sender.connectTimeout;
      this.readTimeout = sender.readTimeout;
      this.compressionEnabled = sender.compressionEnabled;
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
      String endpoint = this.endpoint;
      if (endpoint == null) throw new NullPointerException("endpoint == null");

      HttpEndpointSupplier endpointSupplier = endpointSupplierFactory.create(endpoint);
      if (endpointSupplier == null) {
        throw new NullPointerException("endpointSupplierFactory.create() returned null");
      }
      if (endpointSupplier instanceof HttpEndpointSupplier.Constant) {
        endpoint = endpointSupplier.get(); // eagerly resolve the endpoint
        return new URLConnectionSender(this, new ConstantHttpURLConnectionSupplier(endpoint));
      }
      return new URLConnectionSender(this, new DynamicHttpURLConnectionSupplier(endpointSupplier));
    }

    Builder() {
    }
  }

  static URL toURL(String endpoint) {
    try {
      return new URL(endpoint);
    } catch (MalformedURLException e) {
      throw new IllegalArgumentException(e.getMessage());
    }
  }

  static abstract class HttpURLConnectionSupplier {
    abstract HttpURLConnection openConnection() throws IOException;

    void close() {
    }
  }

  static final class ConstantHttpURLConnectionSupplier extends HttpURLConnectionSupplier {
    final URL url;

    ConstantHttpURLConnectionSupplier(String endpoint) {
      this.url = toURL(endpoint);
    }

    @Override HttpURLConnection openConnection() throws IOException {
      return (HttpURLConnection) url.openConnection();
    }

    @Override public String toString() {
      return url.toString();
    }
  }

  static final class DynamicHttpURLConnectionSupplier extends HttpURLConnectionSupplier {
    final HttpEndpointSupplier endpointSupplier;

    DynamicHttpURLConnectionSupplier(HttpEndpointSupplier endpointSupplier) {
      this.endpointSupplier = endpointSupplier;
    }

    @Override HttpURLConnection openConnection() throws IOException {
      String endpoint = endpointSupplier.get();
      if (endpoint == null) throw new NullPointerException("endpointSupplier.get() returned null");
      URL url = toURL(endpoint);
      return (HttpURLConnection) url.openConnection();
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

  final HttpURLConnectionSupplier connectionSupplier;
  final Encoding encoding;
  final String mediaType;
  final BytesMessageEncoder encoder;
  final int messageMaxBytes;
  final int connectTimeout, readTimeout;
  final boolean compressionEnabled;

  URLConnectionSender(Builder builder, HttpURLConnectionSupplier connectionSupplier) {
    this.endpointSupplierFactory = builder.endpointSupplierFactory; // for toBuilder()
    this.endpoint = builder.endpoint; // for toBuilder()

    this.connectionSupplier = connectionSupplier;
    this.encoding = builder.encoding;
    switch (builder.encoding) {
      case JSON:
        this.mediaType = "application/json";
        this.encoder = BytesMessageEncoder.JSON;
        break;
      case THRIFT:
        this.mediaType = "application/x-thrift";
        this.encoder = BytesMessageEncoder.THRIFT;
        break;
      case PROTO3:
        this.mediaType = "application/x-protobuf";
        this.encoder = BytesMessageEncoder.PROTO3;
        break;
      default:
        throw new UnsupportedOperationException("Unsupported encoding: " + encoding.name());
    }
    this.messageMaxBytes = builder.messageMaxBytes;
    this.connectTimeout = builder.connectTimeout;
    this.readTimeout = builder.readTimeout;
    this.compressionEnabled = builder.compressionEnabled;
  }

  public Builder toBuilder() {
    return new Builder(this);
  }

  /** close is typically called from a different thread */
  final AtomicBoolean closeCalled = new AtomicBoolean();

  @Override public int messageSizeInBytes(List<byte[]> encodedSpans) {
    return encoding().listSizeInBytes(encodedSpans);
  }

  @Override public int messageSizeInBytes(int encodedSizeInBytes) {
    return encoding().listSizeInBytes(encodedSizeInBytes);
  }

  @Override public Encoding encoding() {
    return encoding;
  }

  @Override public int messageMaxBytes() {
    return messageMaxBytes;
  }

  /** {@inheritDoc} */
  @Override @Deprecated public Call<Void> sendSpans(List<byte[]> encodedSpans) {
    if (closeCalled.get()) throw new ClosedSenderException();
    return new HttpPostCall(encoder.encode(encodedSpans));
  }

  /** Sends spans as a POST to {@link Builder#endpoint}. */
  @Override public void send(List<byte[]> encodedSpans) throws IOException {
    if (closeCalled.get()) throw new ClosedSenderException();
    send(encoder.encode(encodedSpans), mediaType);
  }

  /** {@inheritDoc} */
  @Override @Deprecated public CheckResult check() {
    try {
      send(new byte[] {'[', ']'}, "application/json");
      return CheckResult.OK;
    } catch (Throwable e) {
      Call.propagateIfFatal(e);
      return CheckResult.failed(e);
    }
  }

  void send(byte[] body, String mediaType) throws IOException {
    // intentionally not closing the connection, to use keep-alives
    HttpURLConnection connection = connectionSupplier.openConnection();
    connection.setConnectTimeout(connectTimeout);
    connection.setReadTimeout(readTimeout);
    connection.setRequestMethod("POST");
    // Amplification can occur when the Zipkin endpoint is proxied, and the proxy is instrumented.
    // This prevents that in proxies, such as Envoy, that understand B3 single format,
    connection.addRequestProperty("b3", "0");
    connection.addRequestProperty("Content-Type", mediaType);
    if (compressionEnabled) {
      connection.addRequestProperty("Content-Encoding", "gzip");
      ByteArrayOutputStream gzipped = new ByteArrayOutputStream();
      GZIPOutputStream compressor = new GZIPOutputStream(gzipped);
      try {
        compressor.write(body);
      } finally {
        compressor.close();
      }
      body = gzipped.toByteArray();
    }
    connection.setDoOutput(true);
    connection.setFixedLengthStreamingMode(body.length);
    connection.getOutputStream().write(body);

    skipAllContent(connection);
  }

  /** This utility is verbose as we have a minimum java version of 6 */
  static void skipAllContent(HttpURLConnection connection) throws IOException {
    InputStream in = connection.getInputStream();
    IOException thrown = skipAndSuppress(in);
    if (thrown == null) return;
    InputStream err = connection.getErrorStream();
    if (err != null) skipAndSuppress(err); // null is possible, if the connection was dropped
    throw thrown;
  }

  static IOException skipAndSuppress(InputStream in) {
    try {
      while (in.read() != -1) ; // skip
      return null;
    } catch (IOException e) {
      return e;
    } finally {
      try {
        in.close();
      } catch (IOException suppressed) {
      }
    }
  }

  @Override public void close() {
    if (!closeCalled.compareAndSet(false, true)) return; // already closed
    connectionSupplier.close();
  }

  @Override public String toString() {
    return "URLConnectionSender{" + connectionSupplier + "}";
  }

  class HttpPostCall extends Call.Base<Void> {
    private final byte[] message;

    HttpPostCall(byte[] message) {
      this.message = message;
    }

    @Override protected Void doExecute() throws IOException {
      send(message, mediaType);
      return null;
    }

    @Override protected void doEnqueue(Callback<Void> callback) {
      try {
        send(message, mediaType);
        callback.onSuccess(null);
      } catch (Throwable t) {
        Call.propagateIfFatal(t);
        callback.onError(t);
      }
    }

    @Override public Call<Void> clone() {
      return new HttpPostCall(message);
    }
  }
}
