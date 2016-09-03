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
import zipkin.reporter.BytesMessageEncoder;
import zipkin.reporter.Callback;
import zipkin.reporter.Encoding;
import zipkin.reporter.Sender;

import static zipkin.internal.Util.checkNotNull;

/**
 * Reports spans to Zipkin, using its <a href="http://zipkin.io/zipkin-api/#/">POST</a> endpoint.
 *
 * <p>This sender is thread-safe.
 */
@AutoValue
public abstract class URLConnectionSender implements Sender {
  /** Creates a sender that posts {@link Encoding#THRIFT} messages. */
  public static URLConnectionSender create(String endpoint) {
    return builder().endpoint(endpoint).build();
  }

  public static Builder builder() {
    return new AutoValue_URLConnectionSender.Builder()
        .encoding(Encoding.THRIFT)
        .connectTimeout(10 * 1000)
        .readTimeout(60 * 1000)
        .compressionEnabled(true)
        .messageMaxBytes(5 * 1024 * 1024);
  }

  @AutoValue.Builder
  public static abstract class Builder {
    /**
     * No default. The POST URL for zipkin's <a href="http://zipkin.io/zipkin-api/#/">v1 api</a>,
     * usually "http://zipkinhost:9411/api/v1/spans"
     */
    // customizable so that users can re-map /api/v1/spans ex for browser-originated traces
    public final Builder endpoint(String endpoint) {
      checkNotNull(endpoint, "endpoint ex: http://zipkinhost:9411/api/v1/spans");
      try {
        return endpoint(new URL(endpoint));
      } catch (MalformedURLException e) {
        throw new IllegalArgumentException(e.getMessage());
      }
    }

    public abstract Builder endpoint(URL postUrl);

    /** Default 10 * 1000 milliseconds. 0 implies no timeout. */
    public abstract Builder connectTimeout(int connectTimeout);

    /** Default 60 * 1000 milliseconds. 0 implies no timeout. */
    public abstract Builder readTimeout(int readTimeout);

    /** Default true. true implies that spans will be gzipped before transport. */
    public abstract Builder compressionEnabled(boolean compressSpans);

    /** Maximum size of a message. Default 5MiB */
    public abstract Builder messageMaxBytes(int messageMaxBytes);

    /** Controls the "Content-Type" header when sending spans. */
    abstract Builder encoding(Encoding encoding);

    abstract Encoding encoding();

    public final URLConnectionSender build() {
      if (encoding() == Encoding.JSON) {
        return mediaType("application/json").encoder(BytesMessageEncoder.JSON).autoBuild();
      } else if (encoding() == Encoding.THRIFT) {
        return mediaType("application/x-thrift").encoder(BytesMessageEncoder.THRIFT).autoBuild();
      }
      throw new UnsupportedOperationException("Unsupported encoding: " + encoding().name());
    }

    abstract Builder encoder(BytesMessageEncoder encoder);

    abstract Builder mediaType(String mediaType);

    abstract URLConnectionSender autoBuild();

    Builder() {
    }
  }

  public Builder toBuilder() {
    return new AutoValue_URLConnectionSender.Builder(this);
  }

  abstract BytesMessageEncoder encoder();

  abstract URL endpoint();

  abstract int connectTimeout();

  abstract int readTimeout();

  abstract boolean compressionEnabled();

  abstract String mediaType();

  @Override public int messageSizeInBytes(List<byte[]> encodedSpans) {
    return encoding().listSizeInBytes(encodedSpans);
  }

  /** Asynchronously sends the spans as a POST to {@link #endpoint()}. */
  @Override public void sendSpans(List<byte[]> encodedSpans, Callback callback) {
    if (encodedSpans.isEmpty()) {
      callback.onComplete();
      return;
    }
    try {
      byte[] message = encoder().encode(encodedSpans);
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
