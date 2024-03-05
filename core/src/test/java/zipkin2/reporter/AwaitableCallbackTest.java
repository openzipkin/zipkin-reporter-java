/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin2.reporter;

import java.io.IOException;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.failBecauseExceptionWasNotThrown;

class AwaitableCallbackTest {
  @Test void awaitIsUninterruptable() {
    AwaitableCallback captor = new AwaitableCallback();
    Thread thread = new Thread(captor::await);
    thread.start();
    thread.interrupt();

    assertThat(thread.isInterrupted()).isTrue();
    // The callback thread receiving an interrupt has nothing to do with the caller of the captor
    assertThat(Thread.currentThread().isInterrupted()).isFalse();
  }

  @Test void onSuccessReturns() {
    AwaitableCallback captor = new AwaitableCallback();
    captor.onSuccess(null);

    captor.await();
  }

  @Test void onError_propagatesRuntimeException() {
    AwaitableCallback captor = new AwaitableCallback();
    captor.onError(new IllegalStateException());

    assertThatThrownBy(captor::await)
      .isInstanceOf(IllegalStateException.class);
  }

  @Test void onError_propagatesError() {
    AwaitableCallback captor = new AwaitableCallback();
    captor.onError(new LinkageError());

    assertThatThrownBy(captor::await)
      .isInstanceOf(LinkageError.class);
  }

  @Test void onError_doesntSetInterrupted() {
    AwaitableCallback captor = new AwaitableCallback();
    captor.onError(new InterruptedException());

    try {
      captor.await();
      failBecauseExceptionWasNotThrown(RuntimeException.class);
    } catch (RuntimeException e) {
      assertThat(e).hasCauseInstanceOf(InterruptedException.class);
      assertThat(Thread.currentThread().isInterrupted()).isFalse();
    }
  }

  @Test void onError_wrapsCheckedExceptions() {
    AwaitableCallback captor = new AwaitableCallback();
    captor.onError(new IOException());

    assertThatThrownBy(captor::await)
      .isInstanceOf(RuntimeException.class)
      .hasCauseInstanceOf(IOException.class);
  }

  @Test void onError_wrapsCustomThrowable() {
    AwaitableCallback captor = new AwaitableCallback();
    class MyThrowable extends Throwable {
    }
    captor.onError(new MyThrowable());

    assertThatThrownBy(captor::await)
      .isInstanceOf(RuntimeException.class)
      .hasCauseInstanceOf(MyThrowable.class);
  }
}
