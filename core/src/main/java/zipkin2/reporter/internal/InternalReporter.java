/*
 * Copyright 2016-2020 The OpenZipkin Authors
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

import zipkin2.reporter.AsyncReporter;
import zipkin2.reporter.Sender;

/**
 * Escalate internal APIs in {@code zipkin2.reporter} so they can be used from outside packages. The
 * only implementation is in {@link Sender}.
 *
 * <p>Inspired by {@code okhttp3.internal.Internal}.
 */
public abstract class InternalReporter {
  public static InternalReporter instance;

  /**
   * Internal utility that allows a reporter to be reconfigured.
   *
   * <p><em>Note:</em>Call {@link AsyncReporter#close()} if you no longer need this instance, as
   * otherwise it can leak its reporting thread.
   *
   * @since 2.15
   */
  public abstract AsyncReporter.Builder toBuilder(AsyncReporter<?> asyncReporter);
}
