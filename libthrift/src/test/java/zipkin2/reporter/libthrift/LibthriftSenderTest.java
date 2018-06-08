/*
 * Copyright 2016-2018 The OpenZipkin Authors
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
package zipkin2.reporter.libthrift;

import java.io.IOException;
import java.util.List;
import java.util.stream.Stream;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import zipkin2.Span;
import zipkin2.codec.SpanBytesEncoder;
import zipkin2.collector.CollectorMetrics;
import zipkin2.collector.scribe.ScribeCollector;
import zipkin2.storage.InMemoryStorage;

import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.failBecauseExceptionWasNotThrown;
import static zipkin2.reporter.TestObjects.CLIENT_SPAN;

public class LibthriftSenderTest {
  @Rule public ExpectedException thrown = ExpectedException.none();

  InMemoryStorage storage = InMemoryStorage.newBuilder().build();
  ScribeCollector collector;

  @Before
  public void start() {
    collector =
        ScribeCollector.newBuilder()
            .metrics(CollectorMetrics.NOOP_METRICS)
            .storage(storage)
            .build();
    collector.start();
  }

  @After
  public void close() {
    collector.close();
  }

  LibthriftSender sender = LibthriftSender.create("127.0.0.1");

  @Test
  public void sendsSpans() throws Exception {
    send(CLIENT_SPAN, CLIENT_SPAN);

    assertThat(storage.spanStore().getTraces()).containsExactly(asList(CLIENT_SPAN, CLIENT_SPAN));
  }

  @Test
  public void check_okWhenScribeIsListening() {
    assertThat(sender.check().ok()).isTrue();
  }

  @Test
  public void check_notOkWhenScribeIsDown() throws Exception {
    collector.close();

    assertThat(sender.check().ok()).isFalse();
  }

  @Test
  public void reconnects() throws Exception {
    close();
    try {
      send(CLIENT_SPAN, CLIENT_SPAN);
      failBecauseExceptionWasNotThrown(IOException.class);
    } catch (IOException e) {
    }
    start();

    send(CLIENT_SPAN, CLIENT_SPAN);

    assertThat(storage.spanStore().getTraces()).containsExactly(asList(CLIENT_SPAN, CLIENT_SPAN));
  }

  @Test
  public void illegalToSendWhenClosed() throws Exception {
    thrown.expect(IllegalStateException.class);
    sender.close();

    send(CLIENT_SPAN, CLIENT_SPAN);
  }

  /**
   * The output of toString() on {@link zipkin2.reporter.Sender} implementations appears in thread
   * names created by {@link zipkin2.reporter.AsyncReporter}. Since thread names are likely to be
   * exposed in logs and other monitoring tools, care should be taken to ensure the toString()
   * output is a reasonable length and does not contain sensitive information.
   */
  @Test
  public void toStringContainsOnlySenderTypeHostAndPort() {
    assertThat(sender.toString())
        .isEqualTo("LibthriftSender(" + sender.host + ":" + sender.port + ")");
  }

  /** Blocks until the callback completes to allow read-your-writes consistency during tests. */
  void send(Span... spans) throws IOException {
    List<byte[]> encodedSpans =
        Stream.of(spans).map(SpanBytesEncoder.THRIFT::encode).collect(toList());
    sender.sendSpans(encodedSpans).execute();
  }
}
