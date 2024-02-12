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

/**
 * HTTP {@link BytesMessageSender senders} check for this type, and will cache its first value.
 *
 * @since 3.3
 */
public final class ConstantHttpEndpointSupplier implements HttpEndpointSupplier {
  /**
   * HTTP {@link BytesMessageSender sender} builders check for this symbol, and return the input as
   * a {@linkplain ConstantHttpEndpointSupplier} result rather than perform dynamic lookups.
   *
   * @since 3.3
   */
  public static final HttpEndpointSupplier.Factory FACTORY = new Factory() {
    @Override public ConstantHttpEndpointSupplier create(String endpoint) {
      return new ConstantHttpEndpointSupplier(endpoint);
    }
  };

  public static ConstantHttpEndpointSupplier create(String endpoint) {
    return new ConstantHttpEndpointSupplier(endpoint);
  }

  private final String endpoint;

  ConstantHttpEndpointSupplier(String endpoint) {
    if (endpoint == null) throw new NullPointerException("endpoint == null");
    this.endpoint = endpoint;
  }

  @Override public String get() {
    return endpoint;
  }

  @Override public void close() {
  }

  @Override public String toString() {
    return endpoint;
  }
}
