/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin2.reporter.beans;

import zipkin2.reporter.HttpEndpointSupplier;

public enum FakeEndpointSupplier implements HttpEndpointSupplier {
  INSTANCE;

  public static final Factory FACTORY = new Factory() {
    @Override public HttpEndpointSupplier create(String endpoint) {
      return INSTANCE;
    }
  };

  @Override public String get() {
    return null;
  }

  @Override public void close() {
  }
}
