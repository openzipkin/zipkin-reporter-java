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
import java.io.UncheckedIOException;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import zipkin2.reporter.HttpEndpointSupplier.Constant;
import zipkin2.reporter.HttpEndpointSuppliers.RateLimited;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static zipkin2.reporter.HttpEndpointSuppliers.constantFactory;
import static zipkin2.reporter.HttpEndpointSuppliers.newConstant;
import static zipkin2.reporter.HttpEndpointSuppliers.rateLimited;
import static zipkin2.reporter.HttpEndpointSuppliers.rateLimitedFactory;

@ExtendWith(MockitoExtension.class)
class RateLimitingSamplerTest {
  static final long NANOS_PER_SECOND = TimeUnit.SECONDS.toNanos(1);

  @Mock Logger logger;
  @Mock HttpEndpointSupplier delegate;
  @Mock HttpEndpointSupplier.Factory delegateFactory;
  @Mock Supplier<Long> nanoTime;

  @AfterEach void verifyMocks() {
    verifyNoMoreInteractions(logger, delegate, nanoTime);
  }

  RateLimited testRateLimited(int intervalSeconds) {
    return new RateLimited(logger, delegate, intervalSeconds) {
      @Override long nanoTime() {
        return nanoTime.get();
      }
    };
  }

  @Test void rateLimitedFactory_exceptions() {
    assertThatThrownBy(() -> rateLimitedFactory(delegateFactory, 0))
      .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> rateLimitedFactory(delegateFactory, -1))
      .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> rateLimitedFactory(null, 3))
      .isInstanceOf(NullPointerException.class);
  }

  @Test void rateLimitedFactory_constant() {
    assertThat(rateLimitedFactory(constantFactory(), 2))
      .isSameAs(constantFactory());
  }

  @Test void rateLimited_exceptions() {
    assertThatThrownBy(() -> rateLimited(delegate, 0))
      .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> rateLimited(delegate, -1))
      .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> rateLimited(null, 3))
      .isInstanceOf(NullPointerException.class);
  }

  @Test void rateLimited_constant() {
    Constant constant = newConstant("http://localhost:9411/api/v2/spans");
    assertThat(rateLimited(constant, 2)).isSameAs(constant);
  }

  @Test void initialGet() {
    when(delegate.get()).thenReturn("initial");
    when(nanoTime.get()).thenReturn(0L);

    RateLimited rateLimited = testRateLimited(2);
    assertThat(rateLimited.endpoint).isEqualTo("initial");
    assertThat(rateLimited.nanoTimeout).isEqualTo(2 * NANOS_PER_SECOND);
  }

  @Test void dispatchesClose() throws Exception {
    when(delegate.get()).thenReturn("initial");
    when(nanoTime.get()).thenReturn(0L);
    doNothing().when(delegate).close();

    testRateLimited(2).close();
  }

  @Test void propagatesInitialException() {
    UncheckedIOException getException = new UncheckedIOException(new IOException());
    when(delegate.get()).thenThrow(getException);

    // ensure at least one value occurs
    assertThatThrownBy(() -> testRateLimited(2))
      .isSameAs(getException);
  }

  @Test void logsAndAdjustsTimeoutOnException() {
    when(nanoTime.get()).thenReturn(0L);
    when(delegate.get()).thenReturn("initial");

    RateLimited rateLimited = testRateLimited(2);
    long initialNanoTimeout = rateLimited.intervalSeconds * NANOS_PER_SECOND;

    // exception calling get on timeout
    when(nanoTime.get()).thenReturn(initialNanoTimeout);
    RuntimeException getException = new RuntimeException("ouch");
    when(delegate.get()).thenThrow(getException);
    doNothing().when(logger)
      .warning("error from httpEndpointSupplier.get() will retry in 2s: ouch");
    when(delegate.toString()).thenReturn("BadSupplier{}");
    doNothing().when(logger)
      .log(Level.FINE, "exception from: BadSupplier{}", getException);
    when(nanoTime.get()).thenReturn(initialNanoTimeout + 100); // get() failed after 100ns

    // the delegate throws an exception, but we return the old value
    assertThat(rateLimited.get()).isEqualTo("initial");

    // The timeout should be interval+nanoTime *after* get, not before it.
    long nextTimeout = initialNanoTimeout + 100 + rateLimited.intervalSeconds * NANOS_PER_SECOND;
    assertThat(rateLimited.nanoTimeout).isEqualTo(nextTimeout);

    // keeps the old endpoint until the next try
    assertThat(rateLimited.endpoint).isEqualTo("initial");
  }

  @Test void propagatesCloseException() throws Exception {
    when(nanoTime.get()).thenReturn(0L);
    when(delegate.get()).thenReturn("initial");
    IOException closeException = new IOException();
    doThrow(closeException).when(delegate).close();

    RateLimited rateLimited = testRateLimited(2);
    assertThatThrownBy(rateLimited::close)
      .isSameAs(closeException);
  }

  @Test void intervals() {
    when(nanoTime.get()).thenReturn(0L);
    when(delegate.get()).thenReturn("initial");

    RateLimited rateLimited = testRateLimited(2);
    long initialNanoTimeout = rateLimited.intervalSeconds * NANOS_PER_SECOND;
    assertThat(rateLimited.endpoint).isEqualTo("initial");
    assertThat(rateLimited.nanoTimeout).isEqualTo(initialNanoTimeout);

    // exact same time
    when(nanoTime.get()).thenReturn(0L);
    assertThat(rateLimited.get()).isEqualTo("initial");
    assertThat(rateLimited.nanoTimeout).isEqualTo(initialNanoTimeout);

    // just before timeout
    when(nanoTime.get()).thenReturn(initialNanoTimeout - 1);
    assertThat(rateLimited.get()).isEqualTo("initial");
    assertThat(rateLimited.nanoTimeout).isEqualTo(initialNanoTimeout);

    // exactly timeout
    when(nanoTime.get()).thenReturn(initialNanoTimeout);
    when(delegate.get()).thenReturn("next");
    when(nanoTime.get()).thenReturn(initialNanoTimeout + 100); // get() takes 100ns
    assertThat(rateLimited.get()).isEqualTo("next");

    // The timeout should be interval+nanoTime *after* get, not before it.
    // This is important if the time to call get takes longer than intervalSeconds.
    long nextTimeout = initialNanoTimeout + 100 + rateLimited.intervalSeconds * NANOS_PER_SECOND;
    assertThat(rateLimited.nanoTimeout).isEqualTo(nextTimeout);

    // after timeout
    when(nanoTime.get()).thenReturn(nextTimeout + 100);
    when(delegate.get()).thenReturn("again");
    when(nanoTime.get()).thenReturn(nextTimeout + NANOS_PER_SECOND); // get() takes 1s
    assertThat(rateLimited.get()).isEqualTo("again");

    long againTimeout =
      nextTimeout + NANOS_PER_SECOND + rateLimited.intervalSeconds * NANOS_PER_SECOND;
    assertThat(rateLimited.nanoTimeout).isEqualTo(againTimeout);
  }

  @Test void negativeNanos() {
    when(nanoTime.get()).thenReturn(-2 * NANOS_PER_SECOND);
    when(delegate.get()).thenReturn("initial");

    RateLimited rateLimited = testRateLimited(1);
    assertThat(rateLimited.endpoint).isEqualTo("initial");
    assertThat(rateLimited.nanoTimeout).isEqualTo(-NANOS_PER_SECOND);

    // just before timeout
    when(nanoTime.get()).thenReturn(-NANOS_PER_SECOND - 1);
    assertThat(rateLimited.get()).isEqualTo("initial");
    assertThat(rateLimited.nanoTimeout).isEqualTo(-NANOS_PER_SECOND);

    // exactly timeout
    when(nanoTime.get()).thenReturn(-NANOS_PER_SECOND);
    when(delegate.get()).thenReturn("next");
    when(nanoTime.get()).thenReturn(-NANOS_PER_SECOND + 100); // get() takes 100ns
    assertThat(rateLimited.get()).isEqualTo("next");
    assertThat(rateLimited.nanoTimeout).isEqualTo(100);
  }
}
