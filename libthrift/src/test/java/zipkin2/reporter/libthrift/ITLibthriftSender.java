/*
 * Copyright 2016-2020 The OpenZipkin Authors
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
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import zipkin2.Span;
import zipkin2.codec.SpanBytesEncoder;
import zipkin2.collector.InMemoryCollectorMetrics;
import zipkin2.collector.scribe.ScribeCollector;
import zipkin2.storage.InMemoryStorage;

import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.failBecauseExceptionWasNotThrown;
import static zipkin2.TestObjects.CLIENT_SPAN;
import static zipkin2.TestObjects.LOTS_OF_SPANS;

public class ITLibthriftSender {
  InMemoryStorage storage = InMemoryStorage.newBuilder().build();
  InMemoryCollectorMetrics collectorMetrics = new InMemoryCollectorMetrics();
  InMemoryCollectorMetrics scribeCollectorMetrics = collectorMetrics.forTransport("scribe");
  ScribeCollector collector;

  @Before
  public void start() {
    collector =
        ScribeCollector.newBuilder()
            .metrics(collectorMetrics)
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
    send(CLIENT_SPAN);

    assertThat(storage.spanStore().getTraces()).containsExactly(asList(CLIENT_SPAN));
  }

  /** This will help verify sequence ID and response parsing logic works */
  @Test
  public void sendsSpans_multipleTimes() throws Exception {
    for (int i = 0; i < 5; i++) { // Have client send 5 messages
      send(Arrays.copyOfRange(LOTS_OF_SPANS, i, (i * 10) + 10));
    }

    assertThat(storage.getTraces()).flatExtracting(l -> l)
        .contains(Arrays.copyOfRange(LOTS_OF_SPANS, 0, 50));
  }

  @Test
  public void sendsSpansExpectedMetrics() throws Exception {
    send(CLIENT_SPAN, CLIENT_SPAN);

    assertThat(scribeCollectorMetrics.messages()).isEqualTo(1);
    assertThat(scribeCollectorMetrics.messagesDropped()).isZero();
    assertThat(scribeCollectorMetrics.spans()).isEqualTo(2);
    assertThat(scribeCollectorMetrics.spansDropped()).isZero();
    byte[] thrift = SpanBytesEncoder.THRIFT.encode(CLIENT_SPAN);

    // span bytes is cumulative thrift size, not message size
    assertThat(scribeCollectorMetrics.bytes()).isEqualTo(thrift.length * 2);
  }

  @Test
  public void check_okWhenScribeIsListening() {
    assertThat(sender.check().ok()).isTrue();
  }

  @Test
  public void check_notOkWhenScribeIsDown() {
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

    assertThat(storage.spanStore().getTraces()).containsExactly(asList(CLIENT_SPAN));
  }

  @Test
  public void illegalToSendWhenClosed() {
    sender.close();

    assertThatThrownBy(() -> send(CLIENT_SPAN, CLIENT_SPAN))
      .isInstanceOf(IllegalStateException.class);
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
