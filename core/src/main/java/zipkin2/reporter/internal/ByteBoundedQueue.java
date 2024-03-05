/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin2.reporter.internal;

import java.util.Arrays;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Multi-producer, multi-consumer queue that is bounded by both count and size.
 *
 * <p>This is similar to {@link java.util.concurrent.ArrayBlockingQueue} in implementation.
 */
final class ByteBoundedQueue<S> implements SpanWithSizeConsumer<S> {

  final ReentrantLock lock = new ReentrantLock(false);
  final Condition available = lock.newCondition();

  final int maxSize;
  final int maxBytes;

  final S[] elements;
  final int[] sizesInBytes;
  int count;
  int sizeInBytes;
  int writePos;
  int readPos;

  @SuppressWarnings("unchecked") ByteBoundedQueue(int maxSize, int maxBytes) {
    this.elements = (S[]) new Object[maxSize];
    this.sizesInBytes = new int[maxSize];
    this.maxSize = maxSize;
    this.maxBytes = maxBytes;
  }

  /**
   * Returns true if the element could be added or false if it could not due to its size.
   */
  @Override public boolean offer(S next, int nextSizeInBytes) {
    lock.lock();
    try {
      if (count == maxSize) return false;
      if (sizeInBytes + nextSizeInBytes > maxBytes) return false;

      elements[writePos] = next;
      sizesInBytes[writePos++] = nextSizeInBytes;

      if (writePos == maxSize) writePos = 0; // circle back to the front of the array

      count++;
      sizeInBytes += nextSizeInBytes;

      available.signal(); // alert any drainers
      return true;
    } finally {
      lock.unlock();
    }
  }

  /** Blocks for up to nanosTimeout for spans to appear. Then, consume as many as possible. */
  int drainTo(SpanWithSizeConsumer<S> consumer, long nanosTimeout) {
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
    }
  }

  /** Clears the queue unconditionally and returns count of spans cleared. */
  int clear() {
    lock.lock();
    try {
      int result = count;
      count = sizeInBytes = readPos = writePos = 0;
      Arrays.fill(elements, null);
      return result;
    } finally {
      lock.unlock();
    }
  }

  int doDrain(SpanWithSizeConsumer<S> consumer) {
    int drainedCount = 0;
    int drainedSizeInBytes = 0;
    while (drainedCount < count) {
      S next = elements[readPos];
      int nextSizeInBytes = sizesInBytes[readPos];

      if (next == null) break;
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
    sizeInBytes -= drainedSizeInBytes;
    return drainedCount;
  }
}

interface SpanWithSizeConsumer<S> {
  /** Returns true if the element could be added or false if it could not due to its size. */
  boolean offer(S next, int nextSizeInBytes);
}
