/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin2.reporter.okhttp3;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import okhttp3.mockwebserver.Dispatcher;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import okhttp3.mockwebserver.SocketPolicy;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import zipkin2.codec.SpanBytesDecoder;
import zipkin2.reporter.AwaitableCallback;
import zipkin2.reporter.BytesMessageSender;
import zipkin2.reporter.Call;
import zipkin2.reporter.Callback;
import zipkin2.reporter.Encoding;
import zipkin2.reporter.HttpEndpointSupplier;
import zipkin2.reporter.SpanBytesEncoder;
import zipkin2.reporter.okhttp3.OkHttpSenderTest.BaseHttpEndpointSupplier;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static zipkin2.TestObjects.CLIENT_SPAN;
import static zipkin2.reporter.okhttp3.OkHttpSenderTest.sendSpans;

public class ITOkHttpSender { // public for use in src/it
  MockWebServer server = new MockWebServer();

  @AfterEach void closeServer() throws IOException {
    server.close();
  }

  String endpoint = server.url("/api/v2/spans").toString();
  OkHttpSender sender =
    OkHttpSender.newBuilder().endpoint(endpoint).compressionEnabled(false).build();

  @Test void canCustomizeClient() throws Exception {
    sender.close();
    OkHttpSender.Builder builder = sender.toBuilder();
    AtomicBoolean called = new AtomicBoolean();
    builder.clientBuilder().addInterceptor(chain -> {
      called.set(true);
      return chain.proceed(chain.request());
    });
    sender = builder.build();

    server.enqueue(new MockResponse());

    sendSpans(sender, CLIENT_SPAN, CLIENT_SPAN);

    assertThat(called.get()).isTrue();
  }

  @Test void send() throws Exception {
    server.enqueue(new MockResponse());

    sendSpans(sender, CLIENT_SPAN, CLIENT_SPAN);

    // Ensure only one request was sent
    assertThat(server.getRequestCount()).isEqualTo(1);

    // Now, let's read back the spans we sent!
    assertThat(SpanBytesDecoder.JSON_V2.decodeList(server.takeRequest().getBody().readByteArray()))
      .containsExactly(CLIENT_SPAN, CLIENT_SPAN);
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

    assertThatThrownBy(() -> sendSpans(sender, CLIENT_SPAN, CLIENT_SPAN))
      .isInstanceOf(IOException.class);
  }

  @Test void send_PROTO3() throws Exception {
    sender = sender.toBuilder().encoding(Encoding.PROTO3).build();

    server.enqueue(new MockResponse());

    sendSpans(sender, CLIENT_SPAN, CLIENT_SPAN);

    // Ensure only one request was sent
    assertThat(server.getRequestCount()).isEqualTo(1);

    // Now, let's read back the spans we sent!
    assertThat(SpanBytesDecoder.PROTO3.decodeList(server.takeRequest().getBody().readByteArray()))
      .containsExactly(CLIENT_SPAN, CLIENT_SPAN);
  }

  @Test void send_THRIFT() throws Exception {
    sender = sender.toBuilder().encoding(Encoding.THRIFT).build();

    server.enqueue(new MockResponse());

    sendSpans(sender, CLIENT_SPAN, CLIENT_SPAN);

    // Ensure only one request was sent
    assertThat(server.getRequestCount()).isEqualTo(1);

    // Now, let's read back the spans we sent!
    assertThat(SpanBytesDecoder.THRIFT.decodeList(server.takeRequest().getBody().readByteArray()))
      .containsExactly(CLIENT_SPAN, CLIENT_SPAN);
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
    assertThat(requests.get(0).getBodySize())
      .isLessThan(requests.get(1).getBodySize());
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
    assertThat(server.takeRequest().getHeader("Content-Type"))
      .isEqualTo("application/json");
  }

  @Deprecated
  @Test void closeWhileRequestInFlight_graceful() throws Exception {
    server.shutdown(); // shutdown the normal zipkin rule
    sender.close();

    MockWebServer server = new MockWebServer();
    server.setDispatcher(new Dispatcher() {
      @Override public MockResponse dispatch(RecordedRequest request) throws InterruptedException {
        Thread.sleep(700); // finishes within the one-second grace
        return new MockResponse();
      }
    });
    try {
      sender = OkHttpSender.create(server.url("/api/v1/spans").toString());

      AwaitableCallback callback = new AwaitableCallback();

      Call<Void> call = sender.sendSpans(asList(SpanBytesEncoder.JSON_V2.encode(CLIENT_SPAN)));
      new Thread(() ->
        call.enqueue(callback)
      ).start();
      Thread.sleep(100); // make sure the thread starts

      sender.close(); // close while request is in flight

      callback.await(); // shouldn't throw as request completed w/in a second
    } finally {
      server.shutdown();
    }
  }

  @Deprecated
  @Test void noExceptionWhenServerErrors() {
    server.enqueue(new MockResponse().setResponseCode(500));

    sender.sendSpans(Collections.emptyList()).enqueue(new Callback<Void>() {
      @Override public void onSuccess(Void aVoid) {
      }

      @Override public void onError(Throwable throwable) {
      }
    });
  }
}
