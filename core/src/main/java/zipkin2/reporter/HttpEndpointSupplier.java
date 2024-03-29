/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin2.reporter;

import java.io.Closeable;
import java.util.List;

/**
 * HTTP-based {@link BytesMessageSender senders}, such as {@linkplain BaseHttpSender} use this to
 * get the endpoint to POST spans to. For example, http://localhost:9411/api/v2/spans
 *
 * <p>These are created by a {@linkplain Factory}, allows this to be constructed with a
 * potentially-pseudo endpoint passed by configuration.
 *
 * <p>The simplest implementation is a {@linkplain Constant}, which is called only once. The easiest
 * way to create constants is to use {@link HttpEndpointSuppliers#constantFactory()} or
 * {@link HttpEndpointSuppliers#newConstant(String)}.
 *
 * <p>Those using dynamic implementations that make remote calls should consider wrapping with
 * {@link HttpEndpointSuppliers#newRateLimitedFactory(Factory, int)} or
 * {@link HttpEndpointSuppliers#newRateLimited(HttpEndpointSupplier, int)} to avoid excessive errors
 * or overhead by calling the backend in a tight loop.
 *
 * <h3>Implementation Notes</h3>
 *
 * {@linkplain BaseHttpSender} is a convenience type that implements the following logic:
 * <ul>
 *   <li>During build, the sender should invoke the {@linkplain Factory}.</li>
 *   <li>If the result is {@link Constant}, build the sender to use a static value.</li>
 *   <li>Otherwise, call {@link HttpEndpointSupplier#get()} each time
 *       {@linkplain BytesMessageSender#send(List)} is invoked.</li>
 *   <li>Call {@link #close()} once during {@link BytesMessageSender#close()}.</li>
 * </ul>
 *
 * <p>Implement friendly {@code toString()} functions, that include the real endpoint or the one
 * passed to the {@linkplain Factory}.
 *
 * <p>Senders are not called during production requests, rather in time or size bounded loop, in a
 * separate async reporting thread. Implementations that resolve endpoints via remote calls, such as
 * from Eureka, should cache internally to avoid blocking the reporter thread on each loop.
 *
 * <p>Some senders, such as Armeria, may have more efficient and precise endpoint group logic. In
 * scenarios where the sender is known, interfaces here may be used as markers. Doing so can satisfy
 * dependency injection, without limiting an HTTP framework that can handle groups, to a
 * single-endpoint supplier.
 *
 * @see BaseHttpSender
 * @see Constant
 * @see HttpEndpointSuppliers
 * @since 3.3
 */
public interface HttpEndpointSupplier extends Closeable {
  /**
   * HTTP {@link BytesMessageSender senders} check for this type, and will cache its first value.
   *
   * @see HttpEndpointSuppliers#constantFactory()
   * @see HttpEndpointSuppliers#newConstant(String)
   * @since 3.3
   */
  interface Constant extends HttpEndpointSupplier {
  }

  /**
   * Returns a possibly cached endpoint to an HTTP {@link BytesMessageSender sender}.
   *
   * <p>This will be called inside {@linkplain BytesMessageSender#send(List)}, unless this is an
   * instance of {@linkplain Constant}.
   *
   * @since 3.3
   */
  String get();

  /**
   * Factory passed to HTTP {@link BytesMessageSender sender} builders to control resolution of the
   * static endpoint from configuration.
   *
   * <p>Invoke this when building a sender, not during {@linkplain BytesMessageSender#send(List)}.
   *
   * @since 3.3
   */
  interface Factory {

    /**
     * Returns a possibly {@linkplain Constant} endpoint supplier, given a
     * static endpoint from configuration.
     *
     * <p>Note: Some factories may perform I/O to lazy-create a
     * {@linkplain Constant} endpoint.
     *
     * @param endpoint a static HTTP endpoint from configuration. For example,
     *                 http://localhost:9411/api/v2/spans
     */
    HttpEndpointSupplier create(String endpoint);
  }
}
