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
package zipkin.reporter.okhttp3;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
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

  OkHttpSender sender = OkHttpSender.create(zipkinRule.httpUrl() + "/api/v1/spans");

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
    sender = sender.toBuilder().spanEncoding(Encoding.JSON).build();

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

      // we expect the first compressed request to be smaller than the uncompressed one.
      assertThat(requests.get(0).getBodySize())
          .isLessThan(requests.get(1).getBodySize());
    } finally {
      server.shutdown();
    }
  }

  @Test public void mediaTypeBasedOnEncoding() throws Exception {
    zipkinRule.shutdown(); // shutdown the normal zipkin rule
    MockWebServer server = new MockWebServer();
    try {
      sender = sender.toBuilder()
          .endpoint(server.url("/api/v1/spans").toString())
          .spanEncoding(Encoding.JSON)
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

  /** Blocks until the callback completes to allow read-your-writes consistency during tests. */
  void send(List<Span> spans) {
    AwaitableCallback callback = new AwaitableCallback();
    sender.sendSpans(spans.stream().map(Encoder.THRIFT::encode).collect(toList()), callback);
    callback.await();
  }
}
