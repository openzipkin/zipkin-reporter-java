/*
 * Copyright 2016-2019 The OpenZipkin Authors
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
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.failBecauseExceptionWasNotThrown;
import static org.hamcrest.core.Is.isA;

public class AwaitableCallbackTest {

  @Rule public ExpectedException thrown = ExpectedException.none();

  @Test public void awaitIsUninterruptable() {
    AtomicBoolean returned = new AtomicBoolean();
    AwaitableCallback captor = new AwaitableCallback();
    Thread thread = new Thread(() -> {
      captor.await();
      returned.set(true);
    });
    thread.start();
    thread.interrupt();

    assertThat(thread.isInterrupted()).isTrue();
    // The callback thread receiving an interrupt has nothing to do with the caller of the captor
    assertThat(Thread.currentThread().isInterrupted()).isFalse();
    assertThat(returned.get()).isFalse();
  }

  @Test public void onSuccessReturns() {
    AwaitableCallback captor = new AwaitableCallback();
    captor.onSuccess(null);

    captor.await();
  }

  @Test public void onError_propagatesRuntimeException() {
    AwaitableCallback captor = new AwaitableCallback();
    captor.onError(new IllegalStateException());

    thrown.expect(IllegalStateException.class);
    captor.await();
  }

  @Test public void onError_propagatesError() {
    AwaitableCallback captor = new AwaitableCallback();
    captor.onError(new LinkageError());

    thrown.expect(LinkageError.class);
    captor.await();
  }

  @Test public void onError_doesntSetInterrupted() {
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

  @Test public void onError_wrapsCheckedExceptions() {
    AwaitableCallback captor = new AwaitableCallback();
    captor.onError(new IOException());

    thrown.expect(RuntimeException.class);
    thrown.expectCause(isA(IOException.class));
    captor.await();
  }

  @Test public void onError_wrapsCustomThrowable() {
    AwaitableCallback captor = new AwaitableCallback();
    class MyThrowable extends Throwable {
    }
    captor.onError(new MyThrowable());

    thrown.expect(RuntimeException.class);
    thrown.expectCause(isA(MyThrowable.class));
    captor.await();
  }
}
