/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin2.reporter.libthrift;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import zipkin2.Span;
import zipkin2.reporter.AsyncReporter;
import zipkin2.reporter.BytesMessageSender;
import zipkin2.reporter.SpanBytesEncoder;

import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static zipkin2.TestObjects.CLIENT_SPAN;
import static zipkin2.TestObjects.LOTS_OF_SPANS;

@Tag("docker")
@Testcontainers(disabledWithoutDocker = true)
@Timeout(60)
class ITLibthriftSender {
  @Container ZipkinContainer zipkin = new ZipkinContainer();

  LibthriftSender sender;

  @BeforeEach void open() {
    sender = LibthriftSender.newBuilder().
      host(zipkin.host()).
      port(zipkin.scribePort()).build();
  }

  @AfterEach void close() {
    sender.close();
  }

  @Test void send() throws Exception {
    sendSpans(CLIENT_SPAN);

    assertThat(zipkin.get("/api/v2/trace/" + CLIENT_SPAN.traceId()).isSuccessful())
      .isTrue();
  }

  /**
   * This will help verify sequence ID and response parsing logic works
   */
  @Test void send_multipleTimes() throws Exception {
    for (int i = 0; i < 5; i++) { // Have client send 5 messages
      sendSpans(Arrays.copyOfRange(LOTS_OF_SPANS, i, (i * 10) + 10));
    }

    for (int i = 0; i < 5; i++) { // Try the last ID of each
      int index = (i * 10) + 9; // not 10 because range is exclusive
      assertThat(zipkin.get("/api/v2/trace/" + LOTS_OF_SPANS[index].traceId()).isSuccessful())
        .isTrue();
    }
  }

  @Test void emptyOk() throws Exception {
    sender.send(Collections.emptyList());
  }

  @Test void sendFailsWhenScribeIsDown() {
    sender.close();

    // Reconfigure to a valid host but invalid port.
    sender = LibthriftSender.newBuilder().
      host(zipkin.host()).
      port(zipkin.httpPort()).build();

    assertThatThrownBy(() -> sendSpans(CLIENT_SPAN, CLIENT_SPAN))
      .isInstanceOf(IOException.class);
  }

  @Test void reconnects() throws Exception {
    sender.close();

    // Reconfigure to a valid host but port that isn't listening.
    sender = LibthriftSender.newBuilder().
      host(zipkin.host()).
      port(9999).build();

    assertThatThrownBy(() -> sendSpans(CLIENT_SPAN, CLIENT_SPAN))
      .isInstanceOf(IOException.class);

    open();

    sendSpans(CLIENT_SPAN, CLIENT_SPAN);

    assertThat(zipkin.get("/api/v2/trace/" + CLIENT_SPAN.traceId()).isSuccessful())
      .isTrue();
  }

  @Test void illegalToSendWhenClosed() {
    sender.close();

    assertThatThrownBy(() -> sendSpans(CLIENT_SPAN, CLIENT_SPAN))
      .isInstanceOf(IllegalStateException.class);
  }

  /**
   * The output of toString() on {@link BytesMessageSender} implementations appears in thread
   * names created by {@link AsyncReporter}. Since thread names are likely to be
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
  void sendSpans(Span... spans) throws IOException {
    List<byte[]> encodedSpans =
      Stream.of(spans).map(SpanBytesEncoder.THRIFT::encode).collect(toList());
    sender.send(encodedSpans);
  }
}
