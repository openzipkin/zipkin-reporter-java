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
import java.util.zip.GZIPOutputStream;
import zipkin2.reporter.Component;
import zipkin2.reporter.HttpSender;

/**
 * We have to nest this class until v4 when {@linkplain URLConnectionSender} no longer needs to
 * extend {@linkplain Component}.
 */
final class InternalURLConnectionSender extends HttpSender<URL, byte[]> {

  final int messageMaxBytes;
  final int connectTimeout;
  final int readTimeout;
  final boolean compressionEnabled;

  InternalURLConnectionSender(URLConnectionSender.Builder builder) {
    super(builder.encoding, builder.endpointSupplierFactory, builder.endpoint);
    this.messageMaxBytes = builder.messageMaxBytes;
    this.connectTimeout = builder.connectTimeout;
    this.readTimeout = builder.readTimeout;
    this.compressionEnabled = builder.compressionEnabled;
  }

  @Override public int messageMaxBytes() {
    return messageMaxBytes;
  }

  @Override protected URL newEndpoint(String endpoint) {
    try {
      return new URL(endpoint);
    } catch (MalformedURLException e) {
      throw new IllegalArgumentException(e.getMessage());
    }
  }

  @Override protected byte[] newBody(List<byte[]> encodedSpans) {
    return encoding.encode(encodedSpans);
  }

  @Override protected void postSpans(URL endpoint, byte[] body) throws IOException {
    // intentionally not closing the connection, to use keep-alives
    HttpURLConnection connection = (HttpURLConnection) endpoint.openConnection();
    connection.setConnectTimeout(connectTimeout);
    connection.setReadTimeout(readTimeout);
    connection.setRequestMethod("POST");
    // Amplification can occur when the Zipkin endpoint is proxied, and the proxy is instrumented.
    // This prevents that in proxies, such as Envoy, that understand B3 single format,
    connection.addRequestProperty("b3", "0");
    connection.addRequestProperty("Content-Type", encoding.mediaType());
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

  @Override public String toString() {
    return super.toString().replace("Internal", "");
  }
}
