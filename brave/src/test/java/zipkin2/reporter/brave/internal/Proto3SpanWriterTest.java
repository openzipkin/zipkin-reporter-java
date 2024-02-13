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
package zipkin2.reporter.brave.internal;

import brave.Tags;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static zipkin2.reporter.brave.ZipkinSpanConverter.CLIENT_SPAN;
import static zipkin2.reporter.brave.internal.ZipkinProto3FieldsTest.SPAN_FIELD;

public class Proto3SpanWriterTest {
  ZipkinProto3Writer writer = new ZipkinProto3Writer(Tags.ERROR);

  /** proto messages always need a key, so the non-list form is just a single-field */
  @Test void write_startsWithSpanKeyAndLengthPrefix() {
    byte[] bytes = writer.write(CLIENT_SPAN);

    assertThat(bytes)
      .hasSize(writer.sizeInBytes(CLIENT_SPAN))
      .startsWith((byte) 10, SPAN_FIELD.sizeOfValue(CLIENT_SPAN));
  }
}
