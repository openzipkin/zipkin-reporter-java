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

import java.util.ArrayList;
import java.util.List;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class ByteBoundedQueueTest {
  ByteBoundedQueue<byte[]> queue = new ByteBoundedQueue<>(10, 10);

  @Test
  public void offer_failsWhenFull_size() {
    for (int i = 0; i < queue.maxSize; i++) {
      assertThat(queue.offer(new byte[1], 1)).isTrue();
    }
    assertThat(queue.offer(new byte[1], 1)).isFalse();
  }

  @Test
  public void offer_failsWhenFull_sizeInBytes() {
    assertThat(queue.offer(new byte[10], 10)).isTrue();
    assertThat(queue.offer(new byte[1], 1)).isFalse();
  }

  @Test
  public void offer_updatesCount() {
    for (int i = 0; i < queue.maxSize; i++) {
      queue.offer(new byte[1], 1);
    }
    assertThat(queue.count).isEqualTo(10);
  }

  @Test
  public void offer_sizeInBytes() {
    for (int i = 0; i < queue.maxSize; i++) {
      queue.offer(new byte[1], 1);
    }
    assertThat(queue.sizeInBytes).isEqualTo(queue.maxSize);
  }

  @Test
  public void circular() {
    ByteBoundedQueue<Integer> queue = new ByteBoundedQueue<>(10, 10);

    List<Integer> polled = new ArrayList<>();
    SpanWithSizeConsumer<Integer> consumer = (next, ignored) -> polled.add(next);

    // Offer more than the capacity, flushing via poll on interval
    for (int i = 0; i < 15; i++) {
      queue.offer(i, 1);
      queue.drainTo(consumer, 1);
    }

    // ensure we have all of the spans
    assertThat(polled)
        .containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14);
  }
}
