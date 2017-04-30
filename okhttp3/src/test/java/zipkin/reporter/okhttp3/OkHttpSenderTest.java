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
package zipkin.reporter.okhttp3;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import okhttp3.mockwebserver.Dispatcher;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import zipkin.Span;
import zipkin.TestObjects;
import zipkin.junit.HttpFailure;
import zipkin.junit.ZipkinRule;
import zipkin.reporter.Encoder;
import zipkin.reporter.Encoding;
import zipkin.reporter.internal.AwaitableCallback;

import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.failBecauseExceptionWasNotThrown;

public class OkHttpSenderTest {
  static final Span clientSpan = TestObjects.TRACE.get(2);

  @Rule
  public ZipkinRule zipkinRule = new ZipkinRule();
  @Rule
  public ExpectedException thrown = ExpectedException.none();

  String endpoint = zipkinRule.httpUrl() + "/api/v1/spans";
  OkHttpSender sender = OkHttpSender.create(endpoint);

  @Test
  public void badUrlIsAnIllegalArgument() throws Exception {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("invalid post url: ");

    OkHttpSender.create("htp://localhost:9411/api/v1/spans");
  }

  @Test
  public void sendsSpans() throws Exception {
    send(TestObjects.TRACE);

    // Ensure only one request was sent
    assertThat(zipkinRule.httpRequestCount()).isEqualTo(1);

    // Now, let's read back the spans we sent!
    assertThat(zipkinRule.getTraces()).containsExactly(TestObjects.TRACE);
  }

  @Test
  public void sendsSpans_json() throws Exception {
    sender = sender.toBuilder().encoding(Encoding.JSON).build();

    AwaitableCallback callback = new AwaitableCallback();
    sender.sendSpans(asList(Encoder.JSON.encode(clientSpan)), callback);
    callback.await();

    // Ensure only one request was sent
    assertThat(zipkinRule.httpRequestCount()).isEqualTo(1);

    // Now, let's read back the spans we sent!
    assertThat(zipkinRule.getTraces()).containsExactly(asList(clientSpan));
  }

  @Test public void compression() throws Exception {
    zipkinRule.shutdown(); // shutdown the normal zipkin rule

    MockWebServer server = new MockWebServer();
    sender = sender.toBuilder().endpoint(server.url("/api/v1/spans").toString()).build();
    try {
      List<RecordedRequest> requests = new ArrayList<>();
      for (boolean compressionEnabled : asList(true, false)) {
        sender = sender.toBuilder().compressionEnabled(compressionEnabled).build();

        server.enqueue(new MockResponse());

        send(TestObjects.TRACE);

        // block until the request arrived
        requests.add(server.takeRequest());
      }

      RecordedRequest compressedRequest = requests.get(0);

      // We expect the first compressed request to be smaller than the uncompressed one.
      assertThat(compressedRequest.getBodySize())
          .isLessThan(requests.get(1).getBodySize());
      // No need for chunked encoding as we know the length up front.
      assertThat(compressedRequest.getHeader("Content-Length"))
          .isEqualTo(String.valueOf(compressedRequest.getBodySize()));
    } finally {
      server.shutdown();
    }
  }

  @Test public void mediaTypeBasedOnEncoding() throws Exception {
    zipkinRule.shutdown(); // shutdown the normal zipkin rule
    sender.close();

    MockWebServer server = new MockWebServer();
    try {
      sender = sender.toBuilder()
          .endpoint(server.url("/api/v1/spans").toString())
          .encoding(Encoding.JSON)
          .build();

      server.enqueue(new MockResponse());

      send(TestObjects.TRACE); // objects are in json, but we tell it the wrong thing

      // block until the request arrived
      assertThat(server.takeRequest().getHeader("Content-Type"))
          .isEqualTo("application/json");
    } finally {
      server.shutdown();
    }
  }

  @Test public void closeWhileRequestInFlight_cancelsRequest() throws Exception {
    zipkinRule.shutdown(); // shutdown the normal zipkin rule
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

      new Thread(() ->
          sender.sendSpans(asList(Encoder.THRIFT.encode(clientSpan)), callback)
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
  @Test public void messagesSendImmediately() throws Exception {
    zipkinRule.shutdown(); // shutdown the normal zipkin rule
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
        sender.sendSpans(asList(Encoder.THRIFT.encode(clientSpan)), callback1);
        sender.sendSpans(asList(Encoder.THRIFT.encode(clientSpan)), callback2);
      });
      t.start();
      t.join();

      callback1.await();
      callback2.await();
    } finally {
      server.shutdown();
    }
  }

  @Test public void closeWhileRequestInFlight_graceful() throws Exception {
    zipkinRule.shutdown(); // shutdown the normal zipkin rule
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

      new Thread(() ->
          sender.sendSpans(asList(Encoder.THRIFT.encode(clientSpan)), callback)
      ).start();
      Thread.sleep(100); // make sure the thread starts

      sender.close(); // close while request is in flight

      callback.await(); // shouldn't throw as request completed w/in a second
    } finally {
      server.shutdown();
    }
  }

  @Test
  public void onErrorWhenServerErrors() throws Exception {
    zipkinRule.enqueueFailure(HttpFailure.sendErrorResponse(500, "Server Error!"));

    AwaitableCallback callback = new AwaitableCallback();
    sender.sendSpans(asList(Encoder.THRIFT.encode(clientSpan)), callback);

    // shouldn't throw until we call await!
    try {
      callback.await();
      failBecauseExceptionWasNotThrown(IllegalStateException.class);
    } catch (IllegalStateException e) {
    }
  }

  @Test
  public void onErrorWhenServerDisconnects() throws Exception {
    zipkinRule.enqueueFailure(HttpFailure.disconnectDuringBody());

    AwaitableCallback callback = new AwaitableCallback();
    sender.sendSpans(asList(Encoder.THRIFT.encode(clientSpan)), callback);

    // shouldn't throw until we call await!
    try {
      callback.await();
      failBecauseExceptionWasNotThrown(RuntimeException.class);
    } catch (RuntimeException e) {
      assertThat(e.getCause()).isInstanceOf(IOException.class);
    }
  }

  @Test
  public void check_ok() throws Exception {
    assertThat(sender.check().ok).isTrue();

    assertThat(zipkinRule.httpRequestCount()).isEqualTo(1);
  }

  @Test
  public void check_fail() throws Exception {
    zipkinRule.enqueueFailure(HttpFailure.disconnectDuringBody());

    assertThat(sender.check().ok).isFalse();
  }

  @Test
  public void illegalToSendWhenClosed() throws Exception {
    thrown.expect(IllegalStateException.class);
    sender.close();

    send(TestObjects.TRACE);
  }

  /**
   * The output of toString() on {@link zipkin.reporter.Sender} implementations appears in thread
   * names created by {@link zipkin.reporter.AsyncReporter}. Since thread names are likely to be
   * exposed in logs and other monitoring tools, care should be taken to ensure the toString()
   * output is a reasonable length and does not contain sensitive information.
   */
  @Test
  public void toStringContainsOnlySenderTypeAndEndpoint() throws Exception {
    assertThat(sender.toString()).isEqualTo("OkHttpSender(" + endpoint + ")");
  }

  /** Blocks until the callback completes to allow read-your-writes consistency during tests. */
  void send(List<Span> spans) {
    AwaitableCallback callback = new AwaitableCallback();
    sender.sendSpans(spans.stream().map(Encoder.THRIFT::encode).collect(toList()), callback);
    callback.await();
  }
}
