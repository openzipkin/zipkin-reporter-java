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
import java.util.concurrent.TimeUnit;
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
   * {@link HttpSender sender} implementations look for a {@linkplain Constant} to avoid the
   * overhead of dynamic lookups on each call to {@link HttpSender#postSpans(Object, Object)}.
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
   * Returns a {@linkplain Factory} which calls {@link #rateLimited(HttpEndpointSupplier, int)}
   * for each input.
   *
   * @param input           what to rate-limit.
   * @param intervalSeconds amount of time to wait *after* calls to get before trying again.
   * @return a factory that returns rate-limited suppliers unless the input was a
   * {@linkplain HttpEndpointSupplier.Constant}.
   * @throws IllegalArgumentException if intervalSeconds is less than one.
   * @since 3.3
   */
  public static Factory rateLimitedFactory(Factory input, int intervalSeconds) {
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
  public static HttpEndpointSupplier rateLimited(HttpEndpointSupplier input, int intervalSeconds) {
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
        return rateLimited(delegate.create(endpoint), intervalSeconds);
      }

      @Override public String toString() {
        return "RateLimited{delegate=" + delegate + ", intervalSeconds=" + intervalSeconds + "}";
      }
    }

    final Logger logger;
    final HttpEndpointSupplier delegate;
    final int intervalSeconds;

    String endpoint;
    long nextGet;

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
      long now = nanoTime(), updateAt = nextGet;

      // Is it time to call get again?
      long nanosUntilGet = -(now - updateAt); // because nanoTime can be negative
      if (nanosUntilGet <= 0) {
        // See if we won the race, and if so, update the endpoint.
        try {
          endpoint = delegate.get();
        } catch (Throwable t) {
          Call.propagateIfFatal(t);
          logger.warning(format("error getting new endpoint from %s. will retry in %ds: %s",
            delegate, intervalSeconds, t.getMessage()));
        } finally {
          updateNextGet();
        }
      }

      return endpoint;
    }

    private void updateNextGet() {
      // Above may take more than a second, so update the interval again.
      nextGet = nanoTime() + TimeUnit.SECONDS.toNanos(intervalSeconds);
    }

    @Override public final void close() throws IOException {
      delegate.close();
    }

    @Override public String toString() {
      return "RateLimited{delegate=" + delegate + ", intervalSeconds=" + intervalSeconds + "}";
    }
  }
}
