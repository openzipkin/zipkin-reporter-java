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
import zipkin.reporter.internal.JsonBytesMessageEncoder;
import zipkin.reporter.internal.ThriftBytesMessageEncoder;

/**
 * @param <M> encoded form of a message including one or more spans.
 */
public interface MessageEncoder<M> {
  MessageEncoder<byte[]> JSON_BYTES = new JsonBytesMessageEncoder();
  MessageEncoder<byte[]> THRIFT_BYTES = new ThriftBytesMessageEncoder();

  /**
   * Before invoking {@link Sender#sendSpans(List, Callback)}, callers must consider encoding
   * overhead, so as to not exceed {@link Sender#messageMaxBytes()}.
   *
   * <p>Ex. json encoding is typically 2 (for open-closing the array) + spanCount - 1 (for commas)
   */
  int overheadInBytes(int spanCount);

  /**
   * Combines a list of encoded spans into an encoded list. For example, in thrift, this would be
   * length-prefixed, whereas in json, this would be comma-separated and enclosed by brackets.
   *
   * <p>The primary use of this is batch reporting spans. For example, spans are {@link
   * Encoder#encode(Object) encoded} one-by-one into a queue. This queue is drained up to a byte
   * threshold. Then, the list is encoded with this function and reported out-of-process.
   */
  M encode(List<byte[]> encodedSpans);
}
