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
package zipkin2.reporter.internal;

import java.util.List;
import zipkin2.reporter.BytesMessageSender;
import zipkin2.reporter.Encoding;

/** Encodes messages on {@link #send(List)}, but doesn't do anything else. */
final class NoopSender extends BytesMessageSender.Base {
  /** close is typically called from a different thread */
  volatile boolean closeCalled;

  NoopSender(Encoding encoding) {
    super(encoding);
  }

  @Override public int messageMaxBytes() {
    return Integer.MAX_VALUE;
  }

  @Override public void send(List<byte[]> encodedSpans) {
    encoding.encode(encodedSpans);
  }

  @Override public void close() {
    closeCalled = true;
  }

  @Override public String toString() {
    return "NoopSender";
  }
}
