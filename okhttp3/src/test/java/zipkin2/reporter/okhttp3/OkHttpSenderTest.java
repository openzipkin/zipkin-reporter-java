/*
 * Copyright 2016-2023 The OpenZipkin Authors
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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import okhttp3.mockwebserver.Dispatcher;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import okhttp3.mockwebserver.SocketPolicy;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import zipkin2.Call;
import zipkin2.Callback;
import zipkin2.Span;
import zipkin2.codec.Encoding;
import zipkin2.codec.SpanBytesDecoder;
import zipkin2.codec.SpanBytesEncoder;
import zipkin2.reporter.AsyncReporter;
import zipkin2.reporter.AwaitableCallback;
import zipkin2.reporter.Sender;

import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.failBecauseExceptionWasNotThrown;
import static zipkin2.TestObjects.CLIENT_SPAN;

public class OkHttpSenderTest { // public for use in src/it
  MockWebServer server = new MockWebServer();

  @AfterEach void closeServer() throws IOException {
    server.close();
  }

  String endpoint = server.url("/api/v2/spans").toString();
  OkHttpSender sender =
      OkHttpSender.newBuilder().endpoint(endpoint).compressionEnabled(false).build();

  @Test void badUrlIsAnIllegalArgument() {
    assertThatThrownBy(() -> OkHttpSender.create("htp://localhost:9411/api/v1/spans"))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessageStartingWith("invalid POST url: ");
  }

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

    send(CLIENT_SPAN, CLIENT_SPAN).execute();

    assertThat(called.get()).isTrue();
  }

  @Test void sendsSpans() throws Exception {
    server.enqueue(new MockResponse());

    send(CLIENT_SPAN, CLIENT_SPAN).execute();

    // Ensure only one request was sent
    assertThat(server.getRequestCount()).isEqualTo(1);

    // Now, let's read back the spans we sent!
    assertThat(SpanBytesDecoder.JSON_V2.decodeList(server.takeRequest().getBody().readByteArray()))
        .containsExactly(CLIENT_SPAN, CLIENT_SPAN);
  }

  @Test void sendsSpans_PROTO3() throws Exception {
    sender = sender.toBuilder().encoding(Encoding.PROTO3).build();

    server.enqueue(new MockResponse());

    send(CLIENT_SPAN, CLIENT_SPAN).execute();

    // Ensure only one request was sent
    assertThat(server.getRequestCount()).isEqualTo(1);

    // Now, let's read back the spans we sent!
    assertThat(SpanBytesDecoder.PROTO3.decodeList(server.takeRequest().getBody().readByteArray()))
        .containsExactly(CLIENT_SPAN, CLIENT_SPAN);
  }

  @Test void sendsSpans_THRIFT() throws Exception {
    sender = sender.toBuilder().encoding(Encoding.THRIFT).build();

    server.enqueue(new MockResponse());

    send(CLIENT_SPAN, CLIENT_SPAN).execute();

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

      send(CLIENT_SPAN, CLIENT_SPAN).execute();

      // block until the request arrived
      requests.add(server.takeRequest());
    }

    // we expect the first compressed request to be smaller than the uncompressed one.
    assertThat(requests.get(0).getBodySize())
        .isLessThan(requests.get(1).getBodySize());
  }

  @Test void ensuresProxiesDontTrace() throws Exception {
    server.enqueue(new MockResponse());

    send(CLIENT_SPAN, CLIENT_SPAN).execute();

    // If the Zipkin endpoint is proxied and instrumented, it will know "0" means don't trace.
    assertThat(server.takeRequest().getHeader("b3")).isEqualTo("0");
  }

  @Test void mediaTypeBasedOnSpanEncoding() throws Exception {
    server.enqueue(new MockResponse());

    send(CLIENT_SPAN, CLIENT_SPAN).execute();

    // block until the request arrived
    assertThat(server.takeRequest().getHeader("Content-Type"))
        .isEqualTo("application/json");
  }

  @Test void closeWhileRequestInFlight_cancelsRequest() throws Exception {
    server.shutdown(); // shutdown the normal zipkin rule
    sender.close();

    MockWebServer server = new MockWebServer();
    server.setDispatcher(new Dispatcher() {
      @Override public MockResponse dispatch(RecordedRequest request) throws InterruptedException {
        Thread.sleep(2000); // lingers after the one-second grace
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

      try {
        callback.await();
        failBecauseExceptionWasNotThrown(RuntimeException.class);
      } catch (RuntimeException e) {
        // throws because the request still in flight after a second was canceled
        assertThat(e.getCause()).isInstanceOf(IOException.class);
      }
    } finally {
      server.shutdown();
    }
  }

  /**
   * Each message by default is up to 5MiB, make sure these go out of process as soon as they can.
   */
  @Test void messagesSendImmediately() throws Exception {
    server.shutdown(); // shutdown the normal zipkin rule
    sender.close();

    CountDownLatch latch = new CountDownLatch(1);
    MockWebServer server = new MockWebServer();
    server.setDispatcher(new Dispatcher() {
      AtomicInteger count = new AtomicInteger();

      @Override public MockResponse dispatch(RecordedRequest request) throws InterruptedException {
        if (count.incrementAndGet() == 1) {
          latch.await();
        } else {
          latch.countDown();
        }
        return new MockResponse();
      }
    });
    try (OkHttpSender sender = OkHttpSender.create(server.url("/api/v1/spans").toString())) {

      AwaitableCallback callback1 = new AwaitableCallback();
      AwaitableCallback callback2 = new AwaitableCallback();

      Thread t = new Thread(() -> {
        sender.sendSpans(asList(SpanBytesEncoder.JSON_V2.encode(CLIENT_SPAN))).enqueue(callback1);
        sender.sendSpans(asList(SpanBytesEncoder.JSON_V2.encode(CLIENT_SPAN))).enqueue(callback2);
      });
      t.start();
      t.join();

      callback1.await();
      callback2.await();
    } finally {
      server.shutdown();
    }
  }

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

  @Test void noExceptionWhenServerErrors() {
    server.enqueue(new MockResponse().setResponseCode(500));

    send().enqueue(new Callback<Void>() {
      @Override public void onSuccess(Void aVoid) {
      }

      @Override public void onError(Throwable throwable) {
      }
    });
  }

  @Test void outOfBandCancel() {
    HttpCall call = (HttpCall) send(CLIENT_SPAN, CLIENT_SPAN);
    call.cancel();

    assertThat(call.isCanceled()).isTrue();
  }

  @Test void check_ok() {
    server.enqueue(new MockResponse());

    assertThat(sender.check().ok()).isTrue();

    assertThat(server.getRequestCount()).isEqualTo(1);
  }

  @Test void check_fail() {
    server.enqueue(new MockResponse().setSocketPolicy(SocketPolicy.DISCONNECT_AT_START));

    assertThat(sender.check().ok()).isFalse();
  }

  @Test void illegalToSendWhenClosed() {
    sender.close();

    assertThatThrownBy(() -> send(CLIENT_SPAN))
      .isInstanceOf(IllegalStateException.class);
  }

  /**
   * The output of toString() on {@link Sender} implementations appears in thread names created by
   * {@link AsyncReporter}. Since thread names are likely to be exposed in logs and other monitoring
   * tools, care should be taken to ensure the toString() output is a reasonable length and does not
   * contain sensitive information.
   */
  @Test void toStringContainsOnlySenderTypeAndEndpoint() {
    assertThat(sender.toString()).isEqualTo("OkHttpSender{" + endpoint + "}");
  }

  @Test void bugGuardCache() {
    assertThat(sender.client.cache())
        .withFailMessage("senders should not open a disk cache")
        .isNull();
  }

  Call<Void> send(Span... spans) {
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
    return sender.sendSpans(Stream.of(spans).map(bytesEncoder::encode).collect(toList()));
  }
}
