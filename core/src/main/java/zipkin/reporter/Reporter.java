/**
 * Copyright 2016 The OpenZipkin Authors
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
package zipkin.reporter;

import java.util.List;
import zipkin.Span;
import zipkin.collector.Collector;

/**
 * Spans are created in instrumentation, transported out-of-band, and eventually persisted. The span
 * reporter sends spans recorded by instrumentation out of process.
 *
 * <p>This accepts a {@link Callback} to allow bridging to async libraries.
 */
// @FunctionalInterface
public interface Reporter {

  /**
   * A callback of completion or error. Typical use is incrementing spans dropped metrics.
   *
   * <p>This is a bridge to async libraries such as CompletableFuture complete,
   * completeExceptionally.
   *
   * <p>Implementations will call either {@link #onComplete} or {@link #onError}, but not both.
   */
  interface Callback {

    /**
     * Invoked when computation completed successfully.
     *
     * <p>When this is called, {@link #onError} won't be.
     */
    void onComplete();

    /**
     * Invoked when computation completed abnormally.
     *
     * <p>When this is called, {@link #onComplete} won't be.
     */
    void onError(Throwable t);
  }

  /**
   * Reports a list of spans to a transport such as http or Kafka.
   *
   * <p>Usually, this involves {@link zipkin.Codec#writeSpans(List) encoding into json or thrift}
   * and enqueueing them for transport over http or Kafka. The typical end recipient is a zipkin
   * {@link Collector}.
   */
  void report(List<Span> spans, Callback callback);
}
