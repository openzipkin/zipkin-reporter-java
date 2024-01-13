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
package zipkin2.reporter.armeria;

import com.linecorp.armeria.client.WebClient;
import com.linecorp.armeria.common.AggregatedHttpResponse;
import com.linecorp.armeria.common.HttpData;
import com.linecorp.armeria.common.HttpMethod;
import com.linecorp.armeria.common.HttpRequest;
import com.linecorp.armeria.common.MediaType;
import java.io.IOException;
import java.util.List;
import zipkin2.reporter.AsyncReporter;
import zipkin2.reporter.BytesMessageEncoder;
import zipkin2.reporter.BytesMessageSender;
import zipkin2.reporter.ClosedSenderException;
import zipkin2.reporter.Encoding;

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
 * sender = WebClientSender.create("http://127.0.0.1:9411/api/v2/spans");
 * }</pre>
 *
 * <h3>Implementation Notes</h3>
 *
 * <p>This sender is thread-safe.
 */
public final class WebClientSender extends BytesMessageSender.Base {

  /** Creates a sender that posts {@link Encoding#JSON} messages. */
  public static WebClientSender create(String endpoint) {
    return newBuilder(WebClient.of(endpoint)).build();
  }

  public static Builder newBuilder(WebClient client) {
    return new Builder(client);
  }

  public static final class Builder {
    final WebClient client;
    Encoding encoding = Encoding.JSON;
    int messageMaxBytes = 500000;

    Builder(WebClient client) {
      this.client = client;
    }

    Builder(WebClientSender sender) {
      client = sender.client;
      encoding = sender.encoding;
      messageMaxBytes = sender.messageMaxBytes;
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

    /** Maximum size of a message. Default 500KB */
    public Builder messageMaxBytes(int messageMaxBytes) {
      this.messageMaxBytes = messageMaxBytes;
      return this;
    }

    public WebClientSender build() {
      return new WebClientSender(this);
    }
  }

  final WebClient client;
  final BytesMessageEncoder encoder;
  final MediaType mediaType;
  final int messageMaxBytes;

  WebClientSender(Builder builder) {
    super(builder.encoding);
    if (builder.client == null) throw new NullPointerException("client == null");
    client = builder.client;
    switch (builder.encoding) {
      case JSON:
        this.mediaType = MediaType.JSON;
        this.encoder = BytesMessageEncoder.JSON;
        break;
      case THRIFT:
        this.mediaType = MediaType.parse("application/x-thrift");
        this.encoder = BytesMessageEncoder.THRIFT;
        break;
      case PROTO3:
        this.mediaType = MediaType.PROTOBUF;
        this.encoder = BytesMessageEncoder.PROTO3;
        break;
      default:
        throw new UnsupportedOperationException("Unsupported encoding: " + encoding.name());
    }
    messageMaxBytes = builder.messageMaxBytes;
  }

  /** Creates a builder out of this object. */
  public Builder toBuilder() {
    return new Builder(this);
  }

  @Override public int messageMaxBytes() {
    return messageMaxBytes;
  }

  /** close is typically called from a different thread */
  volatile boolean closeCalled;

  /** Sends spans as a POST to the configured endpoint. */
  @Override public void send(List<byte[]> encodedSpans) throws IOException {
    if (closeCalled) throw new ClosedSenderException();
    byte[] body = encoder.encode(encodedSpans);
    HttpRequest request =
      HttpRequest.of(HttpMethod.POST, "", mediaType, HttpData.wrap(body));
    AggregatedHttpResponse response = client.blocking().execute(request);
    try (HttpData content = response.content()) {
      if (!response.status().isSuccess()) {
        if (content.isEmpty()) {
          throw new IOException("response failed: " + response);
        }
        throw new IOException("response for failed: " + content.toStringAscii());
      }
    }
  }

  /** Waits up to a second for in-flight requests to finish before cancelling them */
  @Override public synchronized void close() {
    if (closeCalled) return;
    closeCalled = true;
    // webclient cannot be closed
  }

  @Override public String toString() {
    return "WebClientSender{" + client + "}";
  }
}
