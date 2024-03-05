/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
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
