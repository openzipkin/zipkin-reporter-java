/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin2.reporter.internal;

import org.junit.jupiter.api.Test;
import zipkin2.reporter.Encoding;

import static org.assertj.core.api.Assertions.assertThat;

class BufferNextMessageTest {
  @Test void empty_json() {
    BufferNextMessage<Integer> pending = BufferNextMessage.create(Encoding.JSON, 10, 0L);

    assertThat(pending.bufferFull)
      .isFalse();
    assertThat(pending.messageSizeInBytes)
      .isEqualTo(2 /* [] */);
  }

  @Test void offer_json() {
    BufferNextMessage<Integer> pending = BufferNextMessage.create(Encoding.JSON, 10, 0L);
    pending.offer(1, 1);

    assertThat(pending.bufferFull)
      .isFalse();
    assertThat(pending.messageSizeInBytes)
      .isEqualTo(3 /* [1] */);

    pending.offer(2, 1);

    assertThat(pending.bufferFull)
      .isFalse();
    assertThat(pending.messageSizeInBytes)
      .isEqualTo(5 /* [1,2] */);
  }

  @Test void offerWhenFull_json() {
    BufferNextMessage<Integer> pending = BufferNextMessage.create(Encoding.JSON, 10, 0L);
    for (int i = 0; i < 4; i++) {
      assertThat(pending.offer(i, 1))
        .isTrue();
    }
    // buffer is not quite full
    assertThat(pending.bufferFull)
      .isFalse();
    assertThat(pending.messageSizeInBytes)
      .isEqualTo(9 /* [0,1,2,3] */);

    // but another element will put it over the edge, so drops
    assertThat(pending.offer(4, 1))
      // should drop because 4 implies ",4" which makes the total length 11
      .isFalse();
    // then we should consider buffer is full and drain all
    assertThat(pending.bufferFull).isTrue();
  }

  @Test void drain_json() {
    BufferNextMessage<Integer> pending = BufferNextMessage.create(Encoding.JSON, 10, 0L);
    for (int i = 0; i < 4; i++) {
      pending.offer(i, 1);
    }

    // fully drain
    pending.drain((s, n) -> true);

    // back to initial state
    assertThat(pending).usingRecursiveComparison().isEqualTo(
      BufferNextMessage.create(Encoding.JSON, 10, 0L)
    );
  }

  @Test void drain_incrementally_json() {
    BufferNextMessage<Integer> pending = BufferNextMessage.create(Encoding.JSON, 10, 0L);
    for (int i = 0; i < 4; i++) {
      pending.offer(i, 1);
    }

    // partial drain
    pending.drain((s, n) -> s < 2);

    assertThat(pending.spans)
      .containsExactly(2, 3);
    assertThat(pending.messageSizeInBytes)
      .isEqualTo(5 /* [2,3] */);

    // partial drain again
    pending.drain((s, n) -> s < 3);

    assertThat(pending.spans)
      .containsExactly(3);
    assertThat(pending.messageSizeInBytes)
      .isEqualTo(3 /* [3] */);
  }

  @Test void empty_proto3() {
    BufferNextMessage<Integer> pending = BufferNextMessage.create(Encoding.PROTO3, 10, 0L);

    assertThat(pending.bufferFull)
      .isFalse();
    assertThat(pending.messageSizeInBytes)
      .isZero();
  }

  @Test void offer_proto3() {
    BufferNextMessage<Character> pending = BufferNextMessage.create(Encoding.PROTO3, 10, 0L);
    pending.offer('a', 1);

    assertThat(pending.bufferFull)
      .isFalse();
    assertThat(pending.messageSizeInBytes)
      .isEqualTo(1 /* a */);

    pending.offer('b', 1);

    assertThat(pending.bufferFull)
      .isFalse();
    assertThat(pending.messageSizeInBytes)
      .isEqualTo(2 /* ab */);
  }

  @Test void offerWhenFull_proto3() {
    BufferNextMessage<Integer> pending = BufferNextMessage.create(Encoding.PROTO3, 10, 0L);
    for (int i = 0; i < 3; i++) {
      assertThat(pending.offer(i, 3))
        .isTrue();
    }
    // buffer is not quite full
    assertThat(pending.bufferFull)
      .isFalse();
    assertThat(pending.messageSizeInBytes)
      .isEqualTo(9 /* 012 */);

    // but another element will put it over the edge, so drops
    assertThat(pending.offer(3, 3))
      // should drop because this implies adding 3 bytes which makes the total length 12
      .isFalse();
    // then we should consider buffer is full and drain all
    assertThat(pending.bufferFull).isTrue();
  }

  @Test void drain_proto3() {
    BufferNextMessage<Integer> pending = BufferNextMessage.create(Encoding.PROTO3, 10, 0L);
    for (int i = 0; i < 4; i++) {
      pending.offer(i, 1);
    }

    // fully drain
    pending.drain((s, n) -> true);

    // back to initial state
    assertThat(pending).usingRecursiveComparison().isEqualTo(
      BufferNextMessage.create(Encoding.PROTO3, 10, 0L)
    );
  }

  @Test void drain_incrementally_proto3() {
    BufferNextMessage<Integer> pending = BufferNextMessage.create(Encoding.PROTO3, 10, 0L);
    for (int i = 0; i < 4; i++) {
      pending.offer(i, 1);
    }

    // partial drain
    pending.drain((s, n) -> s < 2);

    assertThat(pending.spans)
      .containsExactly(2, 3);
    assertThat(pending.messageSizeInBytes)
      .isEqualTo(2 /* 23 */);

    // partial drain again
    pending.drain((s, n) -> s < 3);

    assertThat(pending.spans)
      .containsExactly(3);
    assertThat(pending.messageSizeInBytes)
      .isEqualTo(1 /* 3 */);
  }
}
