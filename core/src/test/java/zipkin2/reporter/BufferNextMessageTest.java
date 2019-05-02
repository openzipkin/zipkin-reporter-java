/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package zipkin2.reporter;

import org.junit.Test;
import zipkin2.codec.Encoding;

import static org.assertj.core.api.Assertions.assertThat;

public class BufferNextMessageTest {

  @Test public void empty_json() {
    BufferNextMessage<Integer> pending = BufferNextMessage.create(Encoding.JSON, 10, 0L);

    assertThat(pending.bufferFull)
        .isFalse();
    assertThat(pending.messageSizeInBytes)
        .isEqualTo(2 /* [] */);
  }

  @Test public void offer_json() {
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

  @Test public void offerWhenFull_json() {
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

  @Test public void drain_json() {
    BufferNextMessage<Integer> pending = BufferNextMessage.create(Encoding.JSON, 10, 0L);
    for (int i = 0; i < 4; i++) {
      pending.offer(i, 1);
    }

    // fully drain
    pending.drain((s, n) -> true);

    // back to initial state
    assertThat(pending).isEqualToComparingFieldByField(
        BufferNextMessage.create(Encoding.JSON, 10, 0L)
    );
  }

  @Test public void drain_incrementally_json() {
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

  @Test public void empty_proto3() {
    BufferNextMessage<Integer> pending = BufferNextMessage.create(Encoding.PROTO3, 10, 0L);

    assertThat(pending.bufferFull)
        .isFalse();
    assertThat(pending.messageSizeInBytes)
        .isZero();
  }

  @Test public void offer_proto3() {
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

  @Test public void offerWhenFull_proto3() {
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

  @Test public void drain_proto3() {
    BufferNextMessage<Integer> pending = BufferNextMessage.create(Encoding.PROTO3, 10, 0L);
    for (int i = 0; i < 4; i++) {
      pending.offer(i, 1);
    }

    // fully drain
    pending.drain((s, n) -> true);

    // back to initial state
    assertThat(pending).isEqualToComparingFieldByField(
        BufferNextMessage.create(Encoding.PROTO3, 10, 0L)
    );
  }

  @Test public void drain_incrementally_proto3() {
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
