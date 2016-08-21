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
import zipkin.reporter.internal.JsonListEncoder;
import zipkin.reporter.internal.ThriftListEncoder;

public interface ListEncoder {
  ListEncoder JSON = new JsonListEncoder();
  ListEncoder THRIFT = new ThriftListEncoder();

  /**
   * Combines a list of pre-encoded values into an encoded list. For example, in thrift, this would
   * be length-prefixed, whereas in json, this would be comma-separated and enclosed by brackets.
   *
   * <p>The primary use of this is batch reporting spans. For example, spans are {@link
   * SpanEncoder#encode(Span) encoded} one-by-one into a queue. This queue is drained up to a byte
   * threshold. Then, the list is encoded with this function and reported out-of-process.
   */
  byte[] encode(List<byte[]> values);
}
