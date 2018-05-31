/*
 * Copyright 2016-2018 The OpenZipkin Authors
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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import zipkin2.Callback;

/**
 * Blocks until {@link Callback#onSuccess(Object)} or {@link Callback#onError(Throwable)}.
 */
public final class AwaitableCallback implements Callback<Void> {
  final CountDownLatch countDown = new CountDownLatch(1);
  final AtomicReference<Throwable> throwable = new AtomicReference<>();

  /**
   * Blocks until {@link Callback#onSuccess(Object)} or {@link Callback#onError(Throwable)}.
   *
   * <p>Returns unexceptionally if {@link Callback#onSuccess(Object)} was called. <p>Throws if
   * {@link Callback#onError(Throwable)} was called.
   */
  public void await() {
    boolean interrupted = false;
    try {
      while (true) {
        try {
          countDown.await();
          Object result = throwable.get();
          if (result == null) return;
          if (result instanceof Throwable) {
            if (result instanceof Error) throw (Error) result;
            if (result instanceof RuntimeException) throw (RuntimeException) result;
            // Don't set interrupted status when the callback received InterruptedException
            throw new RuntimeException((Throwable) result);
          }
        } catch (InterruptedException e) {
          interrupted = true;
        }
      }
    } finally {
      if (interrupted) {
        Thread.currentThread().interrupt();
      }
    }
  }

  @Override public void onSuccess(Void ignored) {
    countDown.countDown();
  }

  @Override public void onError(Throwable t) {
    throwable.set(t);
    countDown.countDown();
  }
}