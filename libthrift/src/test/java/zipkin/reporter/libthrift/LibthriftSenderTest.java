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
package zipkin.reporter.libthrift;

import java.util.List;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import zipkin.Span;
import zipkin.TestObjects;
import zipkin.collector.CollectorMetrics;
import zipkin.collector.scribe.ScribeCollector;
import zipkin.reporter.Encoder;
import zipkin.reporter.internal.AwaitableCallback;
import zipkin.storage.InMemoryStorage;
import zipkin.storage.QueryRequest;

import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.failBecauseExceptionWasNotThrown;

public class LibthriftSenderTest {
  @Rule public ExpectedException thrown = ExpectedException.none();

  InMemoryStorage storage = new InMemoryStorage();
  ScribeCollector collector;

  @Before
  public void start() {
    collector = ScribeCollector.builder()
        .metrics(CollectorMetrics.NOOP_METRICS)
        .storage(storage).build();
    collector.start();
  }

  @After
  public void close() {
    collector.close();
  }

  LibthriftSender sender = LibthriftSender.create("127.0.0.1");

  @Test
  public void sendsSpans() throws Exception {
    send(TestObjects.TRACE);

    assertThat(storage.spanStore().getTraces(QueryRequest.builder().build()))
        .containsExactly(TestObjects.TRACE);
  }

  @Test
  public void check_okWhenScribeIsListening() throws Exception {
    assertThat(sender.check().ok)
        .isTrue();
  }

  @Test
  public void check_notOkWhenScribeIsDown() throws Exception {
    collector.close();

    assertThat(sender.check().ok)
        .isFalse();
  }

  @Test
  public void reconnects() throws Exception {
    close();
    try {
      send(TestObjects.TRACE);
      failBecauseExceptionWasNotThrown(RuntimeException.class);
    } catch (RuntimeException e) {
    }
    start();

    send(TestObjects.TRACE);

    assertThat(storage.spanStore().getTraces(QueryRequest.builder().build()))
        .containsExactly(TestObjects.TRACE);
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
  public void toStringContainsOnlySenderTypeHostAndPort() throws Exception {
    assertThat(sender.toString())
        .isEqualTo("LibthriftSender(" + sender.host() + ":" + sender.port() + ")");
  }

  /** Blocks until the callback completes to allow read-your-writes consistency during tests. */
  void send(List<Span> spans) {
    AwaitableCallback callback = new AwaitableCallback();
    sender.sendSpans(spans.stream().map(Encoder.THRIFT::encode).collect(toList()), callback);
    callback.await();
  }
}
