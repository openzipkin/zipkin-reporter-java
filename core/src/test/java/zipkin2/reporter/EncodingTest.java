/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin2.reporter;

import java.util.List;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class EncodingTest {
  @Test void emptyList_json() {
    List<byte[]> encoded = List.of();
    assertThat(Encoding.JSON.encode(encoded))
      .containsExactly('[', ']');
  }

  @Test void singletonList_json() {
    List<byte[]> encoded = List.of(new byte[] {'{', '}'});

    assertThat(Encoding.JSON.encode(encoded))
      .containsExactly('[', '{', '}', ']');
  }

  @Test void multiItemList_json() {
    List<byte[]> encoded = List.of(
      "{\"k\":\"1\"}".getBytes(),
      "{\"k\":\"2\"}".getBytes(),
      "{\"k\":\"3\"}".getBytes()
    );
    assertThat(new String(Encoding.JSON.encode(encoded)))
      .isEqualTo("[{\"k\":\"1\"},{\"k\":\"2\"},{\"k\":\"3\"}]");
  }

  @Test void emptyList_proto3() {
    List<byte[]> encoded = List.of();
    assertThat(Encoding.PROTO3.encode(encoded))
      .isEmpty();
  }

  @Test void singletonList_proto3() {
    List<byte[]> encoded = List.of(new byte[] {1, 1, 'a'});

    assertThat(Encoding.PROTO3.encode(encoded))
      .containsExactly(1, 1, 'a');
  }

  @Test void multiItemList_proto3() {
    List<byte[]> encoded = List.of(
      new byte[] {1, 1, 'a'},
      new byte[] {1, 1, 'b'},
      new byte[] {1, 1, 'c'}
    );
    assertThat(Encoding.PROTO3.encode(encoded)).containsExactly(
      1, 1, 'a',
      1, 1, 'b',
      1, 1, 'c'
    );
  }
}
