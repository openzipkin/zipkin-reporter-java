/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin2.reporter.internal;

import java.io.IOException;
import java.io.UncheckedIOException;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class PlatformTest {
  // Tests run in JDK 11+, so this should always work.
  @Test void uncheckedIOException() {
    IOException ioe = new IOException("drat");
    assertThat(Platform.get().uncheckedIOException(ioe))
      .isInstanceOf(UncheckedIOException.class)
      .hasCause(ioe);
  }
}
