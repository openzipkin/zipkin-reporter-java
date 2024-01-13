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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import okhttp3.mockwebserver.SocketPolicy;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import zipkin2.Span;
import zipkin2.codec.SpanBytesDecoder;
import zipkin2.codec.SpanBytesEncoder;
import zipkin2.reporter.AsyncReporter;
import zipkin2.reporter.BytesMessageSender;
import zipkin2.reporter.Encoding;

import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static zipkin2.TestObjects.CLIENT_SPAN;

class ITWebClientSender {
  MockWebServer server = new MockWebServer();

  @AfterEach void closeServer() throws IOException {
    server.close();
  }

  WebClientSender sender;
  String endpoint = server.url("/api/v2/spans").toString();

  @BeforeEach void setUp() {
    sender = WebClientSender.create(endpoint);
  }

  @Test void badUrlIsAnIllegalArgument() {
    assertThatThrownBy(
      () -> WebClientSender.create("htp://localhost:9411/api/v1/spans")).isInstanceOf(
      IllegalArgumentException.class).hasMessage("unknown protocol: htp");
  }

  @Test void send() throws Exception {
    server.enqueue(new MockResponse());

    sendSpans(CLIENT_SPAN, CLIENT_SPAN);

    // Ensure only one request was sent
    assertThat(server.getRequestCount()).isEqualTo(1);

    // Now, let's read back the spans we sent!
    assertThat(SpanBytesDecoder.JSON_V2.decodeList(
      server.takeRequest().getBody().readByteArray())).containsExactly(CLIENT_SPAN, CLIENT_SPAN);
  }

  @Test void emptyOk() throws Exception {
    server.enqueue(new MockResponse());

    sender.send(Collections.emptyList());

    assertThat(server.getRequestCount()).isEqualTo(1);
  }

  @Test void sendFailsOnDisconnect() {
    server.enqueue(new MockResponse().setSocketPolicy(SocketPolicy.DISCONNECT_AT_START));

    assertThatThrownBy(() -> sendSpans(CLIENT_SPAN, CLIENT_SPAN)).isInstanceOf(IOException.class);
  }

  @Test void send_PROTO3() throws Exception {
    sender = sender.toBuilder().encoding(Encoding.PROTO3).build();

    server.enqueue(new MockResponse());

    sendSpans(CLIENT_SPAN, CLIENT_SPAN);

    // Ensure only one request was sent
    assertThat(server.getRequestCount()).isEqualTo(1);

    // Now, let's read back the spans we sent!
    assertThat(SpanBytesDecoder.PROTO3.decodeList(
      server.takeRequest().getBody().readByteArray())).containsExactly(CLIENT_SPAN, CLIENT_SPAN);
  }

  @Test void send_THRIFT() throws Exception {
    sender = sender.toBuilder().encoding(Encoding.THRIFT).build();

    server.enqueue(new MockResponse());

    sendSpans(CLIENT_SPAN, CLIENT_SPAN);

    // Ensure only one request was sent
    assertThat(server.getRequestCount()).isEqualTo(1);

    // Now, let's read back the spans we sent!
    assertThat(SpanBytesDecoder.THRIFT.decodeList(
      server.takeRequest().getBody().readByteArray())).containsExactly(CLIENT_SPAN, CLIENT_SPAN);
  }

  @Test @Disabled("TODO") void compression() throws Exception {
    List<RecordedRequest> requests = new ArrayList<>();
    for (boolean compressionEnabled : asList(true, false)) {
      // sender = sender.toBuilder().compressionEnabled(compressionEnabled).build();

      server.enqueue(new MockResponse());

      sendSpans(CLIENT_SPAN, CLIENT_SPAN);

      // block until the request arrived
      requests.add(server.takeRequest());
    }

    // we expect the first compressed request to be smaller than the uncompressed one.
    assertThat(requests.get(0).getBodySize()).isLessThan(requests.get(1).getBodySize());
  }

  @Test void ensuresProxiesDontTrace() throws Exception {
    server.enqueue(new MockResponse());

    sendSpans(CLIENT_SPAN, CLIENT_SPAN);

    // If the Zipkin endpoint is proxied and instrumented, it will know "0" means don't trace.
    assertThat(server.takeRequest().getHeader("b3")).isEqualTo("0");
  }

  @Test void mediaTypeBasedOnSpanEncoding() throws Exception {
    server.enqueue(new MockResponse());

    sendSpans(CLIENT_SPAN, CLIENT_SPAN);

    // block until the request arrived
    assertThat(server.takeRequest().getHeader("Content-Type")).isEqualTo("application/json");
  }

  @Test void illegalToSendWhenClosed() {
    sender.close();

    assertThatThrownBy(() -> sendSpans(CLIENT_SPAN)).isInstanceOf(IllegalStateException.class);
  }

  /**
   * The output of toString() on {@link BytesMessageSender} implementations appears in thread names
   * created by {@link AsyncReporter}. Since thread names are likely to be exposed in logs and other
   * monitoring tools, care should be taken to ensure the toString() output is a reasonable length
   * and does not contain sensitive information.
   */
  @Test void toStringContainsOnlySenderTypeAndEndpoint() {
    assertThat(sender.toString()).isEqualTo("URLConnectionSender{" + endpoint + "}");
  }

  void sendSpans(Span... spans) throws IOException {
    SpanBytesEncoder bytesEncoder;
    switch (sender.encoding()) {
      case JSON:
        bytesEncoder = SpanBytesEncoder.JSON_V2;
        break;
      case THRIFT:
        bytesEncoder = SpanBytesEncoder.THRIFT;
        break;
      case PROTO3:
        bytesEncoder = SpanBytesEncoder.PROTO3;
        break;
      default:
        throw new UnsupportedOperationException("encoding: " + sender.encoding());
    }
    sender.send(Stream.of(spans).map(bytesEncoder::encode).collect(toList()));
  }
}
