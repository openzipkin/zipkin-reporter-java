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

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;
import zipkin2.reporter.HttpEndpointSupplier.Factory;

import static zipkin2.reporter.Call.propagateIfFatal;

/**
 * Reports spans to Zipkin, using its <a href="https://zipkin.io/zipkin-api/#/">POST</a> endpoint.
 *
 * <p>Calls to {@linkplain #postSpans(Object, Object)} happen on the same async reporting thread,
 * but {@linkplain #close()} might be called from any thread.
 *
 * @since 3.3
 */
public abstract class HttpSender<U, B> extends BytesMessageSender.Base {
  final Logger logger;
  final HttpEndpointSupplier endpointSupplier;
  final U endpoint;

  /** close is typically called from a different thread */
  final AtomicBoolean closeCalled = new AtomicBoolean();

  /**
   * Called each invocation of {@linkplain #postSpans(Object, Object)}, unless the
   * {@linkplain HttpEndpointSupplier} is a {@linkplain HttpEndpointSupplier.Constant},
   * Implementations should perform any validation needed here.
   *
   * @since 3.3
   */
  protected abstract U newEndpoint(String endpoint);

  /**
   * Creates a new POST body from the encoded spans.
   *
   * <p>Below is the simplest implementation, when {@linkplain HttpSender#<B>} is a byte array.
   * <pre>{@code
   * @Override protected byte[] newBody(List<byte[]> encodedSpans) {
   *   return encoding.encode(encodedSpans);
   * }
   * }</pre>
   *
   * <p>If you need the "Content-Type" value, you can access it via {@link Encoding#mediaType()}.
   *
   * @since 3.3
   */
  protected abstract B newBody(List<byte[]> encodedSpans);

  /**
   * Implement to POST spans to the given endpoint.
   *
   * <p>If you need the "Content-Type" value, you can access it via {@link Encoding#mediaType()}.
   *
   * @since 3.3
   */
  protected abstract void postSpans(U endpoint, B body) throws IOException;

  /**
   * Override to close any resources.
   *
   * @since 3.3
   */
  protected void doClose() {
  }

  protected HttpSender(Encoding encoding, Factory endpointSupplierFactory, String endpoint) {
    this(Logger.getLogger(HttpSender.class.getName()), encoding, endpointSupplierFactory, endpoint);
  }

  HttpSender(Logger logger, Encoding encoding, Factory endpointSupplierFactory, String endpoint) {
    super(encoding);
    this.logger = logger;
    if (endpointSupplierFactory == null) {
      throw new NullPointerException("endpointSupplierFactory == null");
    }
    if (endpoint == null) throw new NullPointerException("endpoint == null");

    HttpEndpointSupplier endpointSupplier = endpointSupplierFactory.create(endpoint);
    if (endpointSupplier == null) {
      throw new NullPointerException("endpointSupplierFactory.create() returned null");
    }
    if (endpointSupplier instanceof HttpEndpointSupplier.Constant) {
      this.endpoint = nextEndpoint(endpointSupplier);
      closeQuietly(endpointSupplier);
      this.endpointSupplier = null;
    } else {
      this.endpoint = null;
      this.endpointSupplier = endpointSupplier;
    }
  }

  final U nextEndpoint(HttpEndpointSupplier endpointSupplier) {
    String endpoint = endpointSupplier.get(); // eagerly resolve the endpoint
    if (endpoint == null) throw new NullPointerException("endpointSupplier.get() returned null");
    return newEndpoint(endpoint);
  }

  /** Defaults to the most common max message size: 512KB. */
  @Override public int messageMaxBytes() {
    return 512 * 1024;
  }

  /** Sends spans as an HTTP POST request. */
  @Override public final void send(List<byte[]> encodedSpans) throws IOException {
    if (closeCalled.get()) throw new ClosedSenderException();
    U endpoint = this.endpoint;
    if (endpoint == null) endpoint = nextEndpoint(endpointSupplier);
    B body = newBody(encodedSpans);
    if (body == null) throw new NullPointerException("newBody(encodedSpans) returned null");
    postSpans(endpoint, newBody(encodedSpans));
  }

  @Override public final void close() {
    if (!closeCalled.compareAndSet(false, true)) return; // already closed
    closeQuietly(endpointSupplier);
    doClose();
  }

  final void closeQuietly(HttpEndpointSupplier endpointSupplier) {
    if (endpointSupplier == null) return;
    try {
      endpointSupplier.close();
    } catch (Throwable t) {
      propagateIfFatal(t);
      logger.fine("ignoring error closing endpoint supplier: " + t.getMessage());
    }
  }

  @Override public String toString() {
    String name = getClass().getSimpleName();
    if (endpoint != null) {
      return name + "{" + endpoint + "}";
    }
    return name + "{" + endpointSupplier + "}";
  }
}
