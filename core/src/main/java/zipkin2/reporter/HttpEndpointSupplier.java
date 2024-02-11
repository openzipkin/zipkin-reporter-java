package zipkin2.reporter;

import java.util.List;

/**
 * HTTP-based {@link BytesMessageSender senders} use this to resolve a potentially-pseudo endpoint
 * passed by configuration to a real endpoint.
 *
 * <p>Senders should consider the special value {@link #FIXED_FACTORY} and the type {@link Fixed} to
 * avoid dynamic lookups when constants will be returned.
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
 * @since 3.3
 */
public interface HttpEndpointSupplier {
  /**
   * HTTP {@link BytesMessageSender sender} builders check for this symbol, and will substitute its
   * input as a fixed endpoint value rather than perform dynamic lookups.
   *
   * @since 3.3
   */
  Factory FIXED_FACTORY = new Factory() {
    @Override public HttpEndpointSupplier create(String endpoint) {
      return new Fixed(endpoint);
    }
  };

  /**
   * Returns a possibly cached endpoint to an HTTP {@link BytesMessageSender sender}.
   *
   * <p>This will be called inside {@linkplain BytesMessageSender#send(List)}, unless this is an
   * instance of {@linkplain Fixed}.
   *
   * @since 3.3
   */
  String get();

  /**
   * Factory passed to HTTP {@link BytesMessageSender sender} builders to control resolution of the
   * static endpoint from configuration.
   *
   * <p>Unless this is {@linkplain #FIXED_FACTORY}, {@linkplain #create(String)} will be deferred to
   * the first call to {@linkplain BytesMessageSender#send(List)}.
   *
   * @since 3.3
   */
  interface Factory {

    /**
     * Returns a possibly {@linkplain Fixed} endpoint supplier, given a static endpoint from
     * configuration.
     *
     * <p>Note: Some factories may perform I/O to lazy-create a {@linkplain Fixed} endpoint.
     *
     * @param endpoint a static HTTP endpoint from configuration. For example,
     *                 http://localhost:9411/api/v2/spans
     */
    HttpEndpointSupplier create(String endpoint);
  }

  /**
   * HTTP {@link BytesMessageSender senders} check for this type, and will cache its first value.
   */
  final class Fixed implements HttpEndpointSupplier {
    private final String endpoint;

    public Fixed(String endpoint) {
      if (endpoint == null) throw new NullPointerException("endpoint == null");
      this.endpoint = endpoint;
    }

    @Override public String get() {
      return endpoint;
    }
  }
}
