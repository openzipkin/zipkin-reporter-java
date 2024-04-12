/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin2.reporter.internal;

import java.util.Arrays;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import zipkin2.reporter.BytesEncoder;
import zipkin2.reporter.BytesMessageSender;
import zipkin2.reporter.ReporterMetrics;

/**
 * Multi-producer, multi-consumer queue that is bounded by count.
 *
 * <p>This is similar to {@link java.util.concurrent.ArrayBlockingQueue} in implementation.
 */
final class CountBoundedQueue<S> extends BoundedQueue<S> {

  final ReentrantLock lock = new ReentrantLock(false);
  final Condition available = lock.newCondition();

  final BytesEncoder<S> encoder;
  final BytesMessageSender sender;
  final ReporterMetrics metrics;
  final int messageMaxBytes;
  final int maxSize;

  final S[] elements;
  int count;
  int writePos;
  int readPos;

  @SuppressWarnings("unchecked") CountBoundedQueue(BytesEncoder<S> encoder,
    BytesMessageSender sender, ReporterMetrics metrics, int messageMaxBytes, int maxSize) {
    this.encoder = encoder;
    this.sender = sender;
    this.metrics = metrics;
    this.messageMaxBytes = messageMaxBytes;
    this.elements = (S[]) new Object[maxSize];
    this.maxSize = maxSize;
  }

  @Override public boolean offer(S next, int nextSizeInBytes) {
    return offer(next);
  }

  /**
   * Returns true if the element could be added or false if it could not due to its size.
   */
  @Override public boolean offer(S next) {
    lock.lock();
    try {
      if (count == maxSize) return false;

      elements[writePos++] = next;

      if (writePos == maxSize) writePos = 0; // circle back to the front of the array

      count++;

      available.signal(); // alert any drainers
      return true;
    } finally {
      lock.unlock();
    }
  }

  /** Blocks for up to nanosTimeout for spans to appear. Then, consume as many as possible. */
  @Override int drainTo(SpanWithSizeConsumer<S> consumer, long nanosTimeout) {
    try {
      // This may be called by multiple threads. If one is holding a lock, another is waiting. We
      // use lockInterruptibly to ensure the one waiting can be interrupted.
      lock.lockInterruptibly();
      try {
        long nanosLeft = nanosTimeout;
        while (count == 0) {
          if (nanosLeft <= 0) return 0;
          nanosLeft = available.awaitNanos(nanosLeft);
        }
        return doDrain(consumer);
      } finally {
        lock.unlock();
      }
    } catch (InterruptedException e) {
      return 0;
    } finally {
        // record after draining reduces the amount of gauge events vs on doing this on report
        metrics.updateQueuedSpans(count);
    }
  }

  /** Clears the queue unconditionally and returns count of spans cleared. */
  @Override public int clear() {
    lock.lock();
    try {
      int result = count;
      count = readPos = writePos = 0;
      Arrays.fill(elements, null);
      return result;
    } finally {
      lock.unlock();
    }
  }

  int doDrain(SpanWithSizeConsumer<S> consumer) {
    int drainedCount = 0;
    while (drainedCount < count) {
      S next = elements[readPos];

      if (next == null) break;

      int nextSizeInBytes = encoder.sizeInBytes(next);
      int messageSizeOfNextSpan = sender.messageSizeInBytes(nextSizeInBytes);
      metrics.incrementSpanBytes(nextSizeInBytes);

      if (messageSizeOfNextSpan > messageMaxBytes) {
        metrics.incrementSpansDropped(1);
      } else if (!consumer.offer(next, nextSizeInBytes)) {
        break;
      }

      drainedCount++;
      elements[readPos] = null;
      if (++readPos == elements.length) readPos = 0; // circle back to the front of the array
    }
    count -= drainedCount;
    return drainedCount;
  }

  @Override int maxSize() {
    return maxSize;
  }
}
