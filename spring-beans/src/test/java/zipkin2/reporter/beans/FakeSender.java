/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin2.reporter.beans;

import java.io.IOException;
import java.util.List;
import zipkin2.reporter.BytesMessageSender;
import zipkin2.reporter.Encoding;

class FakeSender extends BytesMessageSender.Base {
  FakeSender() {
    super(Encoding.JSON);
  }

  @Override public int messageMaxBytes() {
    return 1024;
  }

  @Override public void send(List<byte[]> encodedSpans) {
  }

  @Override public void close() throws IOException {
  }
}
