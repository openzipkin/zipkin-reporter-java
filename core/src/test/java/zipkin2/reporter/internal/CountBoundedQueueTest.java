/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin2.reporter.internal;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

import zipkin2.reporter.BytesEncoder;
import zipkin2.reporter.Encoding;
import zipkin2.reporter.FakeSender;
import zipkin2.reporter.ReporterMetrics;
import zipkin2.reporter.SpanBytesEncoder;

import static org.assertj.core.api.Assertions.assertThat;

class CountBoundedQueueTest {
  CountBoundedQueue<byte[]> queue = new CountBoundedQueue<>(null, null, ReporterMetrics.NOOP_METRICS, 10, 10);

  @Test void offer_failsWhenFull_size() {
    for (int i = 0; i < queue.maxSize; i++) {
      assertThat(queue.offer(new byte[1], 1)).isTrue();
    }
    assertThat(queue.offer(new byte[1], 1)).isFalse();
  }

  @Test void offer_updatesCount() {
    for (int i = 0; i < queue.maxSize; i++) {
      queue.offer(new byte[1], 1);
    }
    assertThat(queue.count).isEqualTo(10);
  }

  @Test void circular() {
    CountBoundedQueue<Integer> queue = new CountBoundedQueue<>(new BytesEncoder<Integer>() {
      @Override public Encoding encoding() {
        throw new UnsupportedOperationException();
      }

      @Override public int sizeInBytes(Integer input) {
        return 4;
      }

      @Override public byte[] encode(Integer input) {
        throw new UnsupportedOperationException();
      }
    }, FakeSender.create(), ReporterMetrics.NOOP_METRICS, 10, 10);

    List<Integer> polled = new ArrayList<>();
    SpanWithSizeConsumer<Integer> consumer = (next, ignored) -> polled.add(next);

    // Offer more than the capacity, flushing via poll on interval
    for (int i = 0; i < 15; i++) {
      queue.offer(i, 1);
      queue.drainTo(consumer, 1);
    }

    // ensure we have all the spans
    assertThat(polled)
      .containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14);
  }
}
