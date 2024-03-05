/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin2.reporter;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import zipkin2.reporter.HttpEndpointSupplier.Factory;

import static java.lang.String.format;

/**
 * Built-in {@link HttpEndpointSupplier} implementations.
 *
 * @since 3.3
 */
public final class HttpEndpointSuppliers {
  enum ConstantFactory implements Factory {
    INSTANCE;

    @Override public HttpEndpointSupplier create(String endpoint) {
      return newConstant(endpoint);
    }

    @Override public String toString() {
      return "ConstantFactory{}";
    }
  }

  /**
   * Returns a {@linkplain Factory} which calls {@link #newConstant(String)} for each input.
   *
   * @since 3.3
   */
  public static Factory constantFactory() {
    return ConstantFactory.INSTANCE;
  }

  /**
   * {@link BaseHttpSender sender} implementations look for a {@linkplain Constant} to avoid the
   * overhead of dynamic lookups on each call to {@link BaseHttpSender#postSpans(Object, Object)}.
   *
   * @since 3.3
   */
  public static HttpEndpointSupplier.Constant newConstant(String endpoint) {
    if (endpoint == null) throw new NullPointerException("endpoint == null");
    return new Constant(endpoint);
  }

  static final class Constant implements HttpEndpointSupplier.Constant {

    final String endpoint;

    Constant(String endpoint) {
      this.endpoint = endpoint;
    }

    @Override public String get() {
      return endpoint;
    }

    @Override public void close() {
    }

    @Override public boolean equals(Object o) {
      if (o == this) return true;
      if (!(o instanceof HttpEndpointSupplier.Constant)) return false;
      HttpEndpointSupplier.Constant that = (HttpEndpointSupplier.Constant) o;
      return endpoint.equals(that.get());
    }

    @Override public int hashCode() {
      return endpoint.hashCode();
    }

    @Override public String toString() {
      return endpoint;
    }
  }

  /**
   * Returns a {@linkplain Factory} which calls {@link #newRateLimited(HttpEndpointSupplier, int)}
   * for each input.
   *
   * @param intervalSeconds amount of time to wait *after* calls to get before trying again.
   * @return a factory that returns rate-limited suppliers unless the input was a
   * {@linkplain HttpEndpointSupplier.Constant}.
   * @throws IllegalArgumentException if intervalSeconds is less than one.
   * @since 3.3
   */
  public static Factory newRateLimitedFactory(Factory input, int intervalSeconds) {
    if (input == null) throw new NullPointerException("input == null");
    if (intervalSeconds < 1) throw new IllegalArgumentException("intervalSeconds < 1");
    if (input == ConstantFactory.INSTANCE) return input;
    return new RateLimited.Factory(input, intervalSeconds);
  }

  /**
   * Dynamic {@linkplain HttpEndpointSupplier}s can guard against abuse by rate-limiting them.
   *
   * <p>Calling this will invoke {@link HttpEndpointSupplier#get()} on the input. This ensures at
   * least one successful call or an error.
   *
   * <p>Note: errors that occur during get will be logged and ignored. This allows the sender to
   * attempt a prior endpoint vs erring on repeat.
   *
   * @param input           what to rate-limit.
   * @param intervalSeconds amount of time to wait *after* calls to get before trying again.
   * @return a rate-limited supplier unless the input was a {@linkplain HttpEndpointSupplier.Constant}.
   * @throws IllegalArgumentException if intervalSeconds is less than one.
   * @since 3.3
   */
  public static HttpEndpointSupplier newRateLimited(HttpEndpointSupplier input, int intervalSeconds) {
    if (input == null) throw new NullPointerException("input == null");
    if (intervalSeconds < 1) throw new IllegalArgumentException("intervalSeconds < 1");
    if (input instanceof HttpEndpointSupplier.Constant) return input;
    return new RateLimited(RateLimited.LOGGER, input, intervalSeconds);
  }

  static class RateLimited implements HttpEndpointSupplier { // not final for testing
    static final Logger LOGGER = Logger.getLogger(RateLimited.class.getName());

    static final class Factory implements HttpEndpointSupplier.Factory {
      final HttpEndpointSupplier.Factory delegate;
      final int intervalSeconds;

      Factory(HttpEndpointSupplier.Factory delegate, int intervalSeconds) {
        this.delegate = delegate;
        this.intervalSeconds = intervalSeconds;
      }

      @Override public HttpEndpointSupplier create(String endpoint) {
        return newRateLimited(delegate.create(endpoint), intervalSeconds);
      }

      @Override public String toString() {
        return "RateLimited{delegate=" + delegate + ", intervalSeconds=" + intervalSeconds + "}";
      }
    }

    final Logger logger;
    final HttpEndpointSupplier delegate;
    final int intervalSeconds;

    String endpoint;
    long nanoTimeout;

    RateLimited(Logger logger, HttpEndpointSupplier delegate, int intervalSeconds) {
      this.logger = logger;
      this.delegate = delegate;
      this.intervalSeconds = intervalSeconds;

      // Make sure at least one value can be obtained.
      endpoint = delegate.get();
      updateNextGet();
    }

    // overridable for tests
    long nanoTime() {
      return System.nanoTime();
    }

    /**
     * This is used on a sender thread, so it doesn't need to be synchronized technically.
     * However, this is to guard against misuse.
     */
    @Override public final synchronized String get() {
      long now = nanoTime(), nanoTimeout = this.nanoTimeout;

      // Is it time to call get again?
      long nanosUntilGet = -(now - nanoTimeout); // because nanoTime can be negative
      if (nanosUntilGet <= 0) {
        // See if we won the race, and if so, update the endpoint.
        try {
          endpoint = delegate.get();
        } catch (Throwable t) {
          Call.propagateIfFatal(t);
          String message = format("error from httpEndpointSupplier.get() will retry in %ds: %s",
            intervalSeconds, t.getMessage());
          logger.warning(message);
          // log the exception at fine/debug level
          logger.log(Level.FINE, "exception from: " + delegate, t);
        } finally {
          updateNextGet();
        }
      }

      return endpoint;
    }

    private void updateNextGet() {
      // Above may take more than a second, so update the interval again.
      nanoTimeout = nanoTime() + TimeUnit.SECONDS.toNanos(intervalSeconds);
    }

    @Override public final void close() throws IOException {
      delegate.close();
    }

    @Override public String toString() {
      return "RateLimited{delegate=" + delegate + ", intervalSeconds=" + intervalSeconds + "}";
    }
  }
}
