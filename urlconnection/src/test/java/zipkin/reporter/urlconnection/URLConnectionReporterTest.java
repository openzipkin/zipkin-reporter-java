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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.junit.Rule;
import org.junit.Test;
import zipkin.Span;
import zipkin.TestObjects;
import zipkin.junit.HttpFailure;
import zipkin.junit.ZipkinRule;
import zipkin.reporter.Reporter.Callback;
import zipkin.reporter.internal.AwaitableCallback;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;

public class URLConnectionReporterTest {

  @Rule
  public ZipkinRule zipkinRule = new ZipkinRule();

  URLConnectionReporter reporter = URLConnectionReporter.builder()
      .postUrl(zipkinRule.httpUrl() + "/api/v1/spans").build();

  @Test
  public void reportsSpans() throws Exception {
    report(TestObjects.TRACE);

    // Ensure only one request was sent
    assertThat(zipkinRule.httpRequestCount()).isEqualTo(1);

    // Now, let's read back the spans we sent!
    assertThat(zipkinRule.getTraces()).containsExactly(TestObjects.TRACE);
  }

  @Test
  public void dynamicUrl() throws Exception {
    zipkinRule.shutdown();

    // make the url dynamically look up the endpoint
    reporter = URLConnectionReporter.builder()
        .postUrl(() -> zipkinRule.httpUrl() + "/api/v1/spans").build();

    // round robin between a few zipkin servers
    for (int i = 0; i < 3; i++) {
      zipkinRule = new ZipkinRule();
      try {
        report(TestObjects.TRACE);
        assertThat(zipkinRule.getTraces()).containsExactly(TestObjects.TRACE);
      } finally {
        zipkinRule.shutdown();
      }
    }
  }

  @Test public void compression() throws Exception {
    zipkinRule.shutdown(); // shutdown the normal zipkin rule

    MockWebServer server = new MockWebServer();
    reporter = reporter.toBuilder().postUrl(server.url("/").toString()).build();
    try {
      List<RecordedRequest> requests = new ArrayList<>();
      for (boolean compressionEnabled : asList(true, false)) {
        reporter = reporter.toBuilder().compressionEnabled(compressionEnabled).build();

        server.enqueue(new MockResponse());

        // write a complete trace so that it gets reported
        report(TestObjects.TRACE);

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

  @Test
  public void noExceptionWhenServerErrors() throws Exception {
    zipkinRule.enqueueFailure(HttpFailure.sendErrorResponse(500, "Server Error!"));

    thenCallbackCatchesTheThrowable();
  }

  @Test
  public void incrementsDroppedSpansWhenServerDisconnects() throws Exception {
    zipkinRule.enqueueFailure(HttpFailure.disconnectDuringBody());

    thenCallbackCatchesTheThrowable();
  }

  void thenCallbackCatchesTheThrowable() {
    AtomicReference<Throwable> t = new AtomicReference<>();
    Callback callback = new Callback() {

      @Override public void onComplete() {

      }

      @Override public void onError(Throwable throwable) {
        t.set(throwable);
      }
    };

    // Default invocation is blocking
    reporter.report(TestObjects.TRACE, callback);

    // We didn't throw, rather, the exception went to the callback.
    assertThat(t.get()).isNotNull();
  }

  /** Blocks until the callback completes to allow read-your-writes consistency during tests. */
  void report(List<Span> spans) {
    AwaitableCallback callback = new AwaitableCallback();
    reporter.report(spans, callback);
    callback.await();
  }
}
