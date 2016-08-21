/**
 * Copyright 2016 The OpenZipkin Authors
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
package zipkin.reporter;

import java.util.ArrayList;
import java.util.List;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class ByteBoundedQueueTest {
  ByteBoundedQueue<Boolean> queue = new ByteBoundedQueue(10, 10);

  @Test
  public void offer_failsWhenFull_size() {
    for (int i = 0; i < queue.maxSize; i++) {
      assertThat(queue.offer(Boolean.TRUE, 1)).isTrue();
    }
    assertThat(queue.offer(Boolean.TRUE, 1)).isFalse();
  }

  @Test
  public void offer_failsWhenFull_sizeInBytes() {
    assertThat(queue.offer(Boolean.TRUE, 10)).isTrue();
    assertThat(queue.offer(Boolean.TRUE, 1)).isFalse();
  }

  @Test
  public void offer_updatesCount() {
    for (int i = 0; i < queue.maxSize; i++) {
      queue.offer(Boolean.TRUE, 1);
    }
    assertThat(queue.count).isEqualTo(10);
  }

  @Test
  public void offer_sizeInBytes() {
    for (int i = 0; i < queue.maxSize; i++) {
      queue.offer(Boolean.TRUE, 1);
    }
    assertThat(queue.sizeInBytes).isEqualTo(queue.maxSize);
  }

  @Test
  public void circular() {
    ByteBoundedQueue<Integer> queue = new ByteBoundedQueue(10, 10);

    List<Integer> polled = new ArrayList<>();
    ByteBoundedQueue.Consumer<Integer> consumer = (buffer, size) -> polled.add(buffer);

    // Offer more than the capacity, flushing via poll on interval
    for (int i = 0; i < 15; i++) {
      queue.offer(i, 1);
      queue.drainTo(consumer, 1);
    }

    // ensure we have all of the elements
    assertThat(polled)
        .containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14);
  }
}
