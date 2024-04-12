/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package zipkin2.reporter.internal;

import zipkin2.reporter.BytesEncoder;
import zipkin2.reporter.BytesMessageSender;
import zipkin2.reporter.ReporterMetrics;

/**
 * Multi-producer, multi-consumer queue that could be bounded by count or/and size.
 */
abstract class BoundedQueue<S> implements SpanWithSizeConsumer<S> {
  static <S> BoundedQueue<S> create(BytesEncoder<S> encoder, BytesMessageSender sender,
    ReporterMetrics metrics, int messageMaxBytes, int maxSize, int maxBytes) {
    if (maxBytes > 0) {
      return new ByteBoundedQueue<S>(encoder, sender, metrics, messageMaxBytes, maxSize, maxBytes);
    } else {
      return new CountBoundedQueue<S>(encoder, sender, metrics, messageMaxBytes, maxSize);
    }
  }

  /**
   * Max element's count of this bounded queue
   */
  abstract int maxSize();

  /**
   * Clear this bounded queue
   */
  abstract int clear();

  /**
   * Drains this bounded queue. Blocks for up to nanosTimeout for spans to appear.
   * Then, consume as many as possible.
   */
  abstract int drainTo(SpanWithSizeConsumer<S> bundler, long remainingNanos);

  /** Returns true if the element could be added or false if it could not. */
  abstract boolean offer(S next);
}

interface SpanWithSizeConsumer<S> {
  /** Returns true if the element could be added or false if it could not due to its size. */
  boolean offer(S next, int nextSizeInBytes);
}

