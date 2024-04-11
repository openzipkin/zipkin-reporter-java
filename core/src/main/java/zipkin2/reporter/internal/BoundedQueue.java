/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin2.reporter.internal;

import java.util.Arrays;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import zipkin2.reporter.ReporterMetrics;

/**
 * Multi-producer, multi-consumer queue that is bounded by count.
 *
 * <p>This is similar to {@link java.util.concurrent.ArrayBlockingQueue} in implementation.
 */
abstract class BoundedQueue<S> implements SpanWithSizeConsumer<S> {
  final ReentrantLock lock = new ReentrantLock(false);
  final Condition available = lock.newCondition();
  final ReporterMetrics metrics;
  final int maxSize;
  final S[] elements;
  int count;
  int writePos;
  int readPos;

  BoundedQueue(ReporterMetrics metrics, int maxSize) {
    this.metrics = metrics;
    this.maxSize = maxSize;
    this.elements = (S[]) new Object[maxSize];
  }

  boolean offer(S next) {
    return offer(next, 0); // unconditionally enqueue
  }

  /**
   * Returns true if the element could be added or false if it could not due to its count.
   */
  @Override public final boolean offer(S next, int nextSizeInBytes) {
    lock.lock();
    try {
      if (!doOffer(next, nextSizeInBytes)) return false;

      available.signal(); // alert any drainers
      return true;
    } finally {
      lock.unlock();
    }
  }

  boolean doOffer(S next, int nextSizeInBytes) {
    if (count == maxSize) return false;

    elements[writePos++] = next;

    if (writePos == maxSize) writePos = 0; // circle back to the front of the array

    count++;
    return true;
  }

  /**
   * Blocks for up to nanosTimeout for spans to appear. Then, consume as many as possible.
   */
  final void drainTo(SpanWithSizeConsumer<S> consumer, long nanosTimeout) {
    try {
      // This may be called by multiple threads. If one is holding a lock, another is waiting. We
      // use lockInterruptibly to ensure the one waiting can be interrupted.
      lock.lockInterruptibly();
      try {
        long nanosLeft = nanosTimeout;
        while (count == 0) {
          if (nanosLeft <= 0) return;
          nanosLeft = available.awaitNanos(nanosLeft);
        }
        doDrain(consumer);
      } finally {
        lock.unlock();
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  /**
   * @return drainedSizeInBytes and drainedCount packed into a long. The upper 32 bits are the
   * drainedSizeInBytes and the lower 32 bits are the drainedCount.
   */
  long doDrain(SpanWithSizeConsumer<S> consumer) {
    int drainedCount = 0;
    int drainedSizeInBytes = 0;
    while (drainedCount < count) {
      S next = elements[readPos];
      int nextSizeInBytes = getSizeInBytes(readPos);

      if (next == null || nextSizeInBytes == -1) break;
      if (consumer.offer(next, nextSizeInBytes)) {
        drainedCount++;
        drainedSizeInBytes += nextSizeInBytes;

        elements[readPos] = null;
        if (++readPos == elements.length) readPos = 0; // circle back to the front of the array
      } else {
        break;
      }
    }
    count -= drainedCount;
    // record after flushing reduces the amount of gauge events vs on doing this on report
    metrics.updateQueuedSpans(count);
    return ((long) drainedSizeInBytes << 32) | (drainedCount & 0xffffffffL);
  }

  /** return -1 if the span at the position is too big to fit in a message */
  abstract int getSizeInBytes(int pos);

  /** Clears the queue unconditionally and returns count of spans cleared. */
  final int clear() {
    lock.lock();
    try {
      return doClear();
    } finally {
      lock.unlock();
    }
  }

  int doClear() {
    int result = count;
    count = readPos = writePos = 0;
    Arrays.fill(elements, null);
    return result;
  }
}

interface SpanWithSizeConsumer<S> {
  /** Returns true if the element could be added or false if it could not due to its size. */
  boolean offer(S next, int nextSizeInBytes);
}
