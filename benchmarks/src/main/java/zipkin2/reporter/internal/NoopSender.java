/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
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
