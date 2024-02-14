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

import org.junit.jupiter.api.Test;
import zipkin2.reporter.HttpEndpointSupplier.Constant;

import static org.assertj.core.api.Assertions.assertThat;
import static zipkin2.reporter.HttpEndpointSuppliers.constantFactory;
import static zipkin2.reporter.HttpEndpointSuppliers.newConstant;

class HttpEndpointSuppliersTest {

  String endpoint = "http://localhost:9411/api/v2/spans";

  @Test void constantFactory_returnsSameValue() {
    assertThat(constantFactory())
      .isSameAs(constantFactory());
  }

  @Test void constantFactory_returnsInputAsAConstant() {
    assertThat(constantFactory().create(endpoint))
      .isEqualTo(newConstant(endpoint));
  }

  @Test void constantFactory_toString() {
    assertThat(constantFactory())
      .hasToString("ConstantFactory{}");
  }

  @Test void newConstant_toString_onlyHasEndpoint() {
    assertThat(newConstant(endpoint))
      .hasToString(endpoint);
  }

  @Test void newConstant_equalsAndHashCode() {
    // same supplier are equivalent.
    Constant supplier = newConstant(endpoint);
    assertThat(supplier).isEqualTo(supplier);
    assertThat(supplier).hasSameHashCodeAs(supplier);

    // same endpoint constants are equivalent.
    Constant sameEndpoint = newConstant(endpoint);
    assertThat(supplier).isEqualTo(sameEndpoint);
    assertThat(sameEndpoint).isEqualTo(supplier);
    assertThat(supplier).hasSameHashCodeAs(sameEndpoint);

    // constants are equivalent to other constants that are equivalent to their
    // endpoints.
    sameEndpoint = new Constant() {
      @Override public String get() {
        return endpoint;
      }

      @Override public void close() {
      }

      @Override public boolean equals(Object o) {
        if (o == this) return true;
        if (!(o instanceof HttpEndpointSupplier.Constant)) return false;
        HttpEndpointSupplier.Constant that = (HttpEndpointSupplier.Constant) o;
        return get().equals(that.get());
      }

      @Override public int hashCode() {
        return get().hashCode();
      }
    };
    assertThat(supplier).isEqualTo(sameEndpoint);
    assertThat(sameEndpoint).isEqualTo(supplier);
    assertThat(supplier).hasSameHashCodeAs(sameEndpoint);

    // different endpoints are not equivalent.
    // Note: If someone wants to use IPs, the need to resolve them before making a constant!
    Constant differentValue = newConstant("http://127.0.0.1:9411/api/v2/spans");
    assertThat(supplier).isNotEqualTo(differentValue);
    assertThat(differentValue).isNotEqualTo(supplier);
    assertThat(supplier.hashCode()).isNotEqualTo(differentValue.hashCode());
  }
}
