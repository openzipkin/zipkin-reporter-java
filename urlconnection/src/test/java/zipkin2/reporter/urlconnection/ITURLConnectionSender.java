/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin2.reporter.urlconnection;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import okhttp3.mockwebserver.SocketPolicy;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import zipkin2.codec.SpanBytesDecoder;
import zipkin2.reporter.BytesMessageSender;
import zipkin2.reporter.Callback;
import zipkin2.reporter.Encoding;
import zipkin2.reporter.HttpEndpointSupplier;
import zipkin2.reporter.urlconnection.URLConnectionSenderTest.BaseHttpEndpointSupplier;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static zipkin2.TestObjects.CLIENT_SPAN;
import static zipkin2.reporter.urlconnection.URLConnectionSenderTest.sendSpans;

class ITURLConnectionSender {
  MockWebServer server = new MockWebServer();

  @AfterEach void closeServer() throws IOException {
    server.close();
  }

  URLConnectionSender sender;
  String endpoint = server.url("/api/v2/spans").toString();

  @BeforeEach void setUp() {
    sender = URLConnectionSender.newBuilder().endpoint(endpoint).compressionEnabled(false).build();
  }

  @Test void send() throws Exception {
    server.enqueue(new MockResponse());

    sendSpans(sender, CLIENT_SPAN, CLIENT_SPAN);

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

  /**
   * This tests that the {@linkplain HttpEndpointSupplier} is only called once per
   * {@link BytesMessageSender#send(List)}.
   */
  @Test void dynamicEndpoint() throws Exception {
    server.enqueue(new MockResponse());
    server.enqueue(new MockResponse());

    AtomicInteger suffix = new AtomicInteger();
    sender.close();
    sender = sender.toBuilder()
      .endpointSupplierFactory(e -> new BaseHttpEndpointSupplier() {
          @Override public String get() {
            return e + "/" + suffix.incrementAndGet();
          }
        }
      )
      .build();

    sender.send(Collections.emptyList());
    sender.send(Collections.emptyList());

    assertThat(server.takeRequest().getPath()).endsWith("/1");
    assertThat(server.takeRequest().getPath()).endsWith("/2");
  }

  @Test void sendFailsOnDisconnect() {
    server.enqueue(new MockResponse().setSocketPolicy(SocketPolicy.DISCONNECT_AT_START));

    assertThatThrownBy(() -> sendSpans(sender, CLIENT_SPAN, CLIENT_SPAN)).isInstanceOf(
      IOException.class);
  }

  @Test void send_PROTO3() throws Exception {
    sender = sender.toBuilder().encoding(Encoding.PROTO3).build();

    server.enqueue(new MockResponse());

    sendSpans(sender, CLIENT_SPAN, CLIENT_SPAN);

    // Ensure only one request was sent
    assertThat(server.getRequestCount()).isEqualTo(1);

    // Now, let's read back the spans we sent!
    assertThat(SpanBytesDecoder.PROTO3.decodeList(
      server.takeRequest().getBody().readByteArray())).containsExactly(CLIENT_SPAN, CLIENT_SPAN);
  }

  @Test void send_THRIFT() throws Exception {
    sender = sender.toBuilder().encoding(Encoding.THRIFT).build();

    server.enqueue(new MockResponse());

    sendSpans(sender, CLIENT_SPAN, CLIENT_SPAN);

    // Ensure only one request was sent
    assertThat(server.getRequestCount()).isEqualTo(1);

    // Now, let's read back the spans we sent!
    assertThat(SpanBytesDecoder.THRIFT.decodeList(
      server.takeRequest().getBody().readByteArray())).containsExactly(CLIENT_SPAN, CLIENT_SPAN);
  }

  @Test void compression() throws Exception {
    List<RecordedRequest> requests = new ArrayList<>();
    for (boolean compressionEnabled : asList(true, false)) {
      sender = sender.toBuilder().compressionEnabled(compressionEnabled).build();

      server.enqueue(new MockResponse());

      sendSpans(sender, CLIENT_SPAN, CLIENT_SPAN);

      // block until the request arrived
      requests.add(server.takeRequest());
    }

    // we expect the first compressed request to be smaller than the uncompressed one.
    assertThat(requests.get(0).getBodySize()).isLessThan(requests.get(1).getBodySize());
  }

  @Test void ensuresProxiesDontTrace() throws Exception {
    server.enqueue(new MockResponse());

    sendSpans(sender, CLIENT_SPAN, CLIENT_SPAN);

    // If the Zipkin endpoint is proxied and instrumented, it will know "0" means don't trace.
    assertThat(server.takeRequest().getHeader("b3")).isEqualTo("0");
  }

  @Test void mediaTypeBasedOnSpanEncoding() throws Exception {
    server.enqueue(new MockResponse());

    sendSpans(sender, CLIENT_SPAN, CLIENT_SPAN);

    // block until the request arrived
    assertThat(server.takeRequest().getHeader("Content-Type")).isEqualTo("application/json");
  }

  @Deprecated @Test void noExceptionWhenServerErrors() {
    server.enqueue(new MockResponse().setResponseCode(500));

    sender.sendSpans(Collections.emptyList()).enqueue(new Callback<Void>() {
      @Override public void onSuccess(Void aVoid) {
      }

      @Override public void onError(Throwable throwable) {
      }
    });
  }
}
