/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin2.reporter.internal;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

import zipkin2.reporter.ReporterMetrics;

import static org.assertj.core.api.Assertions.assertThat;

class ByteBoundedQueueTest {
  ByteBoundedQueue<byte[]> queue = new ByteBoundedQueue<>(null, null, ReporterMetrics.NOOP_METRICS, 10, 10, 10);

  @Test void offer_failsWhenFull_size() {
    for (int i = 0; i < queue.maxSize; i++) {
      assertThat(queue.offer(new byte[1], 1)).isTrue();
    }
    assertThat(queue.offer(new byte[1], 1)).isFalse();
  }

  @Test void offer_failsWhenFull_sizeInBytes() {
    assertThat(queue.offer(new byte[10], 10)).isTrue();
    assertThat(queue.offer(new byte[1], 1)).isFalse();
  }

  @Test void offer_updatesCount() {
    for (int i = 0; i < queue.maxSize; i++) {
      queue.offer(new byte[1], 1);
    }
    assertThat(queue.count).isEqualTo(10);
  }

  @Test void offer_sizeInBytes() {
    for (int i = 0; i < queue.maxSize; i++) {
      queue.offer(new byte[1], 1);
    }
    assertThat(queue.sizeInBytes).isEqualTo(queue.maxSize);
  }

  @Test void circular() {
    ByteBoundedQueue<Integer> queue = new ByteBoundedQueue<>(null, null, ReporterMetrics.NOOP_METRICS, 10, 10, 10);

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
