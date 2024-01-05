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
package zipkin2.reporter.libthrift;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.RegisterExtension;
import zipkin2.Span;
import zipkin2.reporter.SpanBytesEncoder;

import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static zipkin2.TestObjects.CLIENT_SPAN;
import static zipkin2.TestObjects.LOTS_OF_SPANS;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ITLibthriftSender {
  @RegisterExtension ZipkinExtension zipkin = new ZipkinExtension();

  LibthriftSender sender;

  @BeforeEach void open() {
    sender = LibthriftSender.newBuilder().
      host(zipkin.host()).
      port(zipkin.scribePort()).build();
  }

  @AfterEach void close() {
    sender.close();
  }

  @Test void sendsSpans() throws Exception {
    send(CLIENT_SPAN);

    assertThat(zipkin.get("/api/v2/trace/" + CLIENT_SPAN.traceId()).isSuccessful())
      .isTrue();
  }

  /**
   * This will help verify sequence ID and response parsing logic works
   */
  @Test void sendsSpans_multipleTimes() throws Exception {
    for (int i = 0; i < 5; i++) { // Have client send 5 messages
      send(Arrays.copyOfRange(LOTS_OF_SPANS, i, (i * 10) + 10));
    }

    for (int i = 0; i < 5; i++) { // Try the last ID of each
      int index = (i * 10) + 9; // not 10 because range is exclusive
      assertThat(zipkin.get("/api/v2/trace/" + LOTS_OF_SPANS[index].traceId()).isSuccessful())
        .isTrue();
    }
  }

  @Test void check_okWhenScribeIsListening() {
    assertThat(sender.check().ok()).isTrue();
  }

  @Test void check_notOkWhenScribeIsDown() {
    sender.close();

    // Reconfigure to a valid host but invalid port.
    sender = LibthriftSender.newBuilder().
      host(zipkin.host()).
      port(zipkin.httpPort()).build();

    assertThat(sender.check().ok()).isFalse();
  }

  @Test void reconnects() throws Exception {
    sender.close();

    // Reconfigure to a valid host but port that isn't listening.
    sender = LibthriftSender.newBuilder().
      host(zipkin.host()).
      port(9999).build();

    assertThatThrownBy(() -> send(CLIENT_SPAN, CLIENT_SPAN))
      .isInstanceOf(IOException.class);

    open();

    send(CLIENT_SPAN, CLIENT_SPAN);

    assertThat(zipkin.get("/api/v2/trace/" + CLIENT_SPAN.traceId()).isSuccessful())
      .isTrue();
  }

  @Test void illegalToSendWhenClosed() {
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
  @Test void toStringContainsOnlySenderTypeHostAndPort() {
    assertThat(sender.toString())
      .isEqualTo("LibthriftSender(" + sender.host + ":" + sender.port + ")");
  }

  /**
   * Blocks until the callback completes to allow read-your-writes consistency during tests.
   */
  void send(Span... spans) throws IOException {
    List<byte[]> encodedSpans =
      Stream.of(spans).map(SpanBytesEncoder.THRIFT::encode).collect(toList());
    sender.sendSpans(encodedSpans).execute();
  }
}
