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

class FakeHttpSender extends HttpSender<String, byte[]> {
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
