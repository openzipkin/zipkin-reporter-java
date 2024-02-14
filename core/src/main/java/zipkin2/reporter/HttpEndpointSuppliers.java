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

import zipkin2.reporter.HttpEndpointSupplier.Factory;

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

  /** Returns a {@linkplain Factory} which calls {@link #newConstant(String)} for each input. */
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
}
