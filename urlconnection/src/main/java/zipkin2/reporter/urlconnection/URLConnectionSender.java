/**
 * Copyright 2016-2017 The OpenZipkin Authors
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

import com.google.auto.value.AutoValue;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;
import java.util.zip.GZIPOutputStream;
import zipkin2.Call;
import zipkin2.Callback;
import zipkin2.CheckResult;
import zipkin2.codec.Encoding;
import zipkin2.reporter.BytesMessageEncoder;
import zipkin2.reporter.Sender;
import zipkin2.reporter.internal.BaseCall;

/**
 * Reports spans to Zipkin, using its <a href="http://zipkin.io/zipkin-api/#/">POST</a> endpoint.
 *
 * <p>This sender is thread-safe.
 */
@AutoValue
public abstract class URLConnectionSender extends Sender {
  /** Creates a sender that posts {@link Encoding#JSON} messages. */
  public static URLConnectionSender create(String endpoint) {
    return newBuilder().endpoint(endpoint).build();
  }

  public static Builder newBuilder() {
    return new AutoValue_URLConnectionSender.Builder()
        .encoding(Encoding.JSON)
        .connectTimeout(10 * 1000)
        .readTimeout(60 * 1000)
        .compressionEnabled(true)
        .messageMaxBytes(5 * 1024 * 1024);
  }

  @AutoValue.Builder
  public static abstract class Builder {
    /**
     * No default. The POST URL for zipkin's <a href="http://zipkin.io/zipkin-api/#/">v2 api</a>,
     * usually "http://zipkinhost:9411/api/v2/spans"
     */
    // customizable so that users can re-map /api/v2/spans ex for browser-originated traces
    public final Builder endpoint(String endpoint) {
      if (endpoint == null) throw new NullPointerException("endpoint == null");

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
    public abstract Builder encoding(Encoding encoding);

    abstract Encoding encoding();

    public final URLConnectionSender build() {
      if (encoding() == Encoding.JSON) {
        return mediaType("application/json").encoder(BytesMessageEncoder.JSON).autoBuild();
      }
      throw new UnsupportedOperationException("Unsupported encoding: " + encoding().name());
    }

    abstract Builder encoder(BytesMessageEncoder encoder);

    abstract Builder mediaType(String mediaType);

    abstract URLConnectionSender autoBuild();

    Builder() {
    }
  }

  public abstract Builder toBuilder();

  abstract BytesMessageEncoder encoder();

  abstract URL endpoint();

  abstract int connectTimeout();

  abstract int readTimeout();

  abstract boolean compressionEnabled();

  abstract String mediaType();

  /** close is typically called from a different thread */
  volatile boolean closeCalled;

  @Override public int messageSizeInBytes(List<byte[]> encodedSpans) {
    return encoding().listSizeInBytes(encodedSpans);
  }

  @Override public int messageSizeInBytes(int encodedSizeInBytes) {
    return encoding().listSizeInBytes(encodedSizeInBytes);
  }

  /** The returned call sends spans as a POST to {@link #endpoint()}. */
  @Override public Call<Void> sendSpans(List<byte[]> encodedSpans) {
    if (closeCalled) throw new IllegalStateException("close");
    return new HttpPostCall(encoder().encode(encodedSpans));
  }

  /** Sends an empty json message to the configured endpoint. */
  @Override public CheckResult check() {
    try {
      send(new byte[] {'[', ']'}, "application/json");
      return CheckResult.OK;
    } catch (Throwable e) {
      Call.propagateIfFatal(e);
      return CheckResult.failed(e);
    }
  }

  @Override public void close() {
    closeCalled = true;
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

  @Override public final String toString() {
    return "URLConnectionSender{" + endpoint() + "}";
  }

  URLConnectionSender() {
  }

  class HttpPostCall extends BaseCall<Void> {
    private final byte[] message;

    HttpPostCall(byte[] message) {
      this.message = message;
    }

    @Override protected Void doExecute() throws IOException {
      send(message, mediaType());
      return null;
    }

    @Override protected void doEnqueue(Callback<Void> callback) {
      try {
        send(message, mediaType());
        callback.onSuccess(null);
      } catch (IOException | RuntimeException | Error e) {
        callback.onError(e);
      }
    }

    @Override public Call<Void> clone() {
      return new HttpPostCall(message);
    }
  }
}
