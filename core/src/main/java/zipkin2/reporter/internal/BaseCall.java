/**
 * Copyright 2016-2017 The OpenZipkin Authors
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
package zipkin2.reporter.internal;

import java.io.IOException;
import zipkin2.Call;
import zipkin2.Callback;

public abstract class BaseCall<V> extends Call<V> {
  volatile boolean canceled;
  boolean executed;

  protected BaseCall() {
  }

  @Override public final V execute() throws IOException {
    synchronized (this) {
      if (this.executed) {
        throw new IllegalStateException("Already Executed");
      }

      this.executed = true;
    }

    if (isCanceled()) {
      throw new IOException("Canceled");
    } else {
      return this.doExecute();
    }
  }

  protected abstract V doExecute() throws IOException;

  @Override public final void enqueue(Callback<V> callback) {
    synchronized (this) {
      if (this.executed) {
        throw new IllegalStateException("Already Executed");
      }

      this.executed = true;
    }

    if (isCanceled()) {
      callback.onError(new IOException("Canceled"));
    } else {
      this.doEnqueue(callback);
    }
  }

  protected abstract void doEnqueue(Callback<V> callback);

  @Override public final void cancel() {
    this.canceled = true;
    doCancel();
  }

  protected void doCancel() {
  }

  @Override public final boolean isCanceled() {
    return this.canceled || doIsCanceled();
  }

  protected boolean doIsCanceled() {
    return false;
  }
}
