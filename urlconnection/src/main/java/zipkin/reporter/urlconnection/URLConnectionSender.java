/**
 * Copyright 2016 The OpenZipkin Authors
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
package zipkin.reporter.urlconnection;

import com.google.auto.value.AutoValue;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;
import java.util.zip.GZIPOutputStream;
import zipkin.reporter.Callback;
import zipkin.reporter.Encoding;
import zipkin.reporter.MessageEncoder;
import zipkin.reporter.Sender;

import static zipkin.internal.Util.checkNotNull;

/**
 * Reports spans to Zipkin, using its <a href="http://zipkin.io/zipkin-api/#/">POST</a> endpoint.
 */
@AutoValue
public abstract class URLConnectionSender<B> implements Sender<B> {
  /** Creates a sender that posts {@link MessageEncoder#THRIFT_BYTES thrift} messages. */
  public static URLConnectionSender<byte[]> create(String endpoint) {
    return builder().messageEncoder(MessageEncoder.THRIFT_BYTES).endpoint(endpoint).build();
  }

  public static Builder builder() {
    return new AutoValue_URLConnectionSender.Builder()
        .connectTimeout(10 * 1000)
        .readTimeout(60 * 1000)
        .compressionEnabled(true)
        .messageMaxBytes(5 * 1024 * 1024);
  }

  @AutoValue.Builder
  public static abstract class Builder<B> {
    /**
     * No default. The POST URL for zipkin's <a href="http://zipkin.io/zipkin-api/#/">v1 api</a>,
     * usually "http://zipkinhost:9411/api/v1/spans"
     */
    // customizable so that users can re-map /api/v1/spans ex for browser-originated traces
    public final Builder<B> endpoint(String endpoint) {
      checkNotNull(endpoint, "endpoint ex: http://zipkinhost:9411/api/v1/spans");
      try {
        return endpoint(new URL(endpoint));
      } catch (MalformedURLException e) {
        throw new IllegalArgumentException(e.getMessage());
      }
    }

    public abstract Builder<B> endpoint(URL postUrl);

    /** Default 10 * 1000 milliseconds. 0 implies no timeout. */
    public abstract Builder<B> connectTimeout(int connectTimeout);

    /** Default 60 * 1000 milliseconds. 0 implies no timeout. */
    public abstract Builder<B> readTimeout(int readTimeout);

    /** Default true. true implies that spans will be gzipped before transport. */
    public abstract Builder<B> compressionEnabled(boolean compressSpans);

    /** Maximum size of a message. Default 5MiB */
    public abstract Builder<B> messageMaxBytes(int messageMaxBytes);

    /**
     * Controls the "Content-Type" header and {@link MessageEncoder#encode(List) message encoding}
     * when sending spans.
     */
    public abstract Builder<B> messageEncoder(MessageEncoder<B, byte[]> messageEncoder);

    abstract Builder<B> mediaType(String mediaType);

    abstract MessageEncoder<B, byte[]> messageEncoder();

    abstract Builder<B> messageEncoding(MessageEncoding messageEncoding);

    public final URLConnectionSender<B> build() {
      Encoding encoding = checkNotNull(messageEncoder().encoding(), "messageEncoder.encoding");
      switch (encoding) {
        case JSON:
          mediaType("application/json");
          break;
        case THRIFT:
          mediaType("application/x-thrift");
          break;
        default:
          throw new UnsupportedOperationException("Unsupported encoding: " + encoding.name());
      }
      return messageEncoding(messageEncoder()).autoBuild();
    }

    abstract URLConnectionSender<B> autoBuild();

    Builder() {
    }
  }

  public Builder toBuilder() {
    return new AutoValue_URLConnectionSender.Builder(this);
  }

  abstract URL endpoint();

  abstract int connectTimeout();

  abstract int readTimeout();

  abstract boolean compressionEnabled();

  abstract String mediaType();

  abstract MessageEncoder<B, byte[]> messageEncoder();

  /** Asynchronously sends the spans as a POST to {@link #endpoint()}. */
  @Override public void sendSpans(List<B> encodedSpans, Callback callback) {
    if (encodedSpans.isEmpty()) {
      callback.onComplete();
      return;
    }
    try {
      byte[] message = messageEncoder().encode(encodedSpans);
      send(message, mediaType());
      callback.onComplete();
    } catch (Throwable e) {
      callback.onError(e);
      if (e instanceof Error) throw (Error) e;
    }
  }

  /** Sends an empty json message to the configured endpoint. */
  @Override public CheckResult check() {
    try {
      send(new byte[] {'[', ']'}, "application/json");
      return CheckResult.OK;
    } catch (Exception e) {
      return CheckResult.failed(e);
    }
  }

  @Override public void close() {
    // Nothing to close
  }

  void send(byte[] body, String mediaType) throws IOException {
    // intentionally not closing the connection, so as to use keep-alives
    HttpURLConnection connection = (HttpURLConnection) endpoint().openConnection();
    connection.setConnectTimeout(connectTimeout());
    connection.setReadTimeout(readTimeout());
    connection.setRequestMethod("POST");
    connection.addRequestProperty("Content-Type", mediaType);
    if (compressionEnabled()) {
      connection.addRequestProperty("Content-Encoding", "gzip");
      ByteArrayOutputStream gzipped = new ByteArrayOutputStream();
      try (GZIPOutputStream compressor = new GZIPOutputStream(gzipped)) {
        compressor.write(body);
      }
      body = gzipped.toByteArray();
    }
    connection.setDoOutput(true);
    connection.setFixedLengthStreamingMode(body.length);
    connection.getOutputStream().write(body);

    try (InputStream in = connection.getInputStream()) {
      while (in.read() != -1) ; // skip
    } catch (IOException e) {
      try (InputStream err = connection.getErrorStream()) {
        if (err != null) { // possible, if the connection was dropped
          while (err.read() != -1) ; // skip
        }
      }
      throw e;
    }
  }

  URLConnectionSender() {
  }
}
