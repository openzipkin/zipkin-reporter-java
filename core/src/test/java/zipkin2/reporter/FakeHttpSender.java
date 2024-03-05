/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin2.reporter;

import java.net.MalformedURLException;
import java.net.URI;
import java.util.List;
import java.util.function.Consumer;
import java.util.logging.Logger;
import zipkin2.Span;
import zipkin2.codec.SpanBytesDecoder;
import zipkin2.reporter.HttpEndpointSupplier.Factory;

import static zipkin2.reporter.HttpEndpointSuppliers.constantFactory;

class FakeHttpSender extends BaseHttpSender<String, byte[]> {
  final String originalEndpoint;
  final Consumer<List<Span>> onSpans;

  FakeHttpSender(Logger logger, String endpoint, Consumer<List<Span>> onSpans) {
    this(logger, endpoint, constantFactory(), onSpans);
  }

  FakeHttpSender(Logger logger, String endpoint, Factory endpointSupplierFactory,
    Consumer<List<Span>> onSpans) {
    super(logger, Encoding.JSON, endpointSupplierFactory, endpoint);
    this.originalEndpoint = endpoint;
    this.onSpans = onSpans;
  }

  FakeHttpSender withHttpEndpointSupplierFactory(Factory endpointSupplierFactory) {
    return new FakeHttpSender(logger, originalEndpoint, endpointSupplierFactory, onSpans);
  }

  /** close is typically called from a different thread */
  volatile boolean closeCalled;

  @Override protected String newEndpoint(String endpoint) {
    try {
      return URI.create(endpoint).toURL().toString(); // validate
    } catch (MalformedURLException e) {
      throw new IllegalArgumentException(e.getMessage());
    }
  }

  @Override protected byte[] newBody(List<byte[]> encodedSpans) {
    return encoding.encode(encodedSpans);
  }

  @Override protected void postSpans(String endpoint, byte[] body) {
    List<Span> decoded = SpanBytesDecoder.JSON_V2.decodeList(body);
    onSpans.accept(decoded);
  }

  @Override protected void doClose() {
    closeCalled = true;
  }
}
