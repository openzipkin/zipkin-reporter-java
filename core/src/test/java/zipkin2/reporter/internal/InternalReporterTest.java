/*
 * Copyright 2016-2023 The OpenZipkin Authors
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
import org.junit.Test;
import zipkin2.codec.BytesEncoder;
import zipkin2.codec.Encoding;
import zipkin2.reporter.AsyncReporter;
import zipkin2.reporter.FakeSender;
import zipkin2.reporter.Sender;

import static org.assertj.core.api.Assertions.assertThat;

public class InternalReporterTest {
  Sender sender = FakeSender.create();
  BytesEncoder<String> bytesEncoder = new BytesEncoder<String>() {
    @Override public Encoding encoding() {
      return Encoding.JSON;
    }

    @Override public int sizeInBytes(String s) {
      return s.length();
    }

    @Override public byte[] encode(String s) {
      throw new UnsupportedOperationException();
    }

    @Override public byte[] encodeList(List<String> list) {
      throw new UnsupportedOperationException();
    }
  };

  /**
   * Shows usage for {@code AsyncZipkinSpanHandler} which internally builds an async reporter with a
   * custom bytes encoder. This allows {@code AsyncZipkinSpanHandler.toBuilder()} to be safely
   * created as the there is no ambiguity on whether {@link AsyncReporter.Builder#build()} or {@link
   * AsyncReporter.Builder#build(BytesEncoder)} was called.
   */
  @Test public void toBuilder() {
    AsyncReporter<String> input = AsyncReporter.builder(sender).build(bytesEncoder);
    assertThat(InternalReporter.instance.toBuilder(input).build(bytesEncoder))
        .usingRecursiveComparison()
        .ignoringFields("close", "pending") // for JRE 21
        .isEqualTo(input);
  }
}
