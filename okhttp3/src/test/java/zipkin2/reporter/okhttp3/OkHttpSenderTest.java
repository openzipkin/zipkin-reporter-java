/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin2.reporter.okhttp3;

import java.io.IOException;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import zipkin2.Span;
import zipkin2.codec.SpanBytesEncoder;
import zipkin2.reporter.AsyncReporter;
import zipkin2.reporter.BytesMessageSender;
import zipkin2.reporter.HttpEndpointSupplier;

import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static zipkin2.TestObjects.CLIENT_SPAN;
import static zipkin2.reporter.HttpEndpointSuppliers.constantFactory;
import static zipkin2.reporter.HttpEndpointSuppliers.newConstant;

class OkHttpSenderTest {
  // We can be pretty certain Zipkin isn't listening on localhost port 19092
  OkHttpSender sender = OkHttpSender.newBuilder()
    .readTimeout(100).endpoint("http://localhost:19092").build();

  @Test void toBuilder() {
    sender.close();

    // Change the supplier, but not the endpoint.
    sender = sender.toBuilder()
      .endpointSupplierFactory(e -> newConstant("http://localhost:29092"))
      .build();
    assertThat(sender)
      .hasToString("OkHttpSender{http://localhost:29092/}");

    // Change the supplier, and see the prior endpoint.
    sender = sender.toBuilder()
      .endpointSupplierFactory(constantFactory())
      .build();
    assertThat(sender)
      .hasToString("OkHttpSender{http://localhost:19092/}");

    // Change the endpoint.
    sender = sender.toBuilder()
      .endpoint("http://localhost:29092")
      .build();
    assertThat(sender)
      .hasToString("OkHttpSender{http://localhost:29092/}");
  }

  @Test void sendFailsWhenEndpointIsDown() {
    // Depending on JRE, this could be a ConnectException or a SocketException.
    // Assert IOException to satisfy both!
    assertThatThrownBy(() -> sendSpans(sender, CLIENT_SPAN, CLIENT_SPAN))
      .isInstanceOf(IOException.class);
  }

  /**
   * The output of toString() on {@link BytesMessageSender} implementations appears in thread names
   * created by {@link AsyncReporter}. Since thread names are likely to be exposed in logs and other
   * monitoring tools, care should be taken to ensure the toString() output is a reasonable length
   * and does not contain sensitive information.
   */
  @Test void toStringContainsOnlySummaryInformation() {
    assertThat(sender).hasToString("OkHttpSender{http://localhost:19092/}");
  }

  @Test void bugGuardCache() {
    assertThat(sender.delegate.client.cache())
      .withFailMessage("senders should not open a disk cache")
      .isNull();
  }

  static void sendSpans(BytesMessageSender sender, Span... spans) throws IOException {
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
    sender.send(Stream.of(spans).map(bytesEncoder::encode).collect(toList()));
  }

  static abstract class BaseHttpEndpointSupplier implements HttpEndpointSupplier {
    @Override public void close() {
    }
  }
}
