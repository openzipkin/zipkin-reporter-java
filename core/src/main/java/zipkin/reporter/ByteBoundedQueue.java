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

import java.util.Arrays;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Multi-producer, multi-consumer queue that is bounded by both count and size.
 *
 * <p>This is similar to {@link java.util.concurrent.ArrayBlockingQueue} in implementation.
 */
final class ByteBoundedQueue {

  interface Consumer {
    /** Returns true if it accepted the next element */
    boolean accept(byte[] next);
  }

  final ReentrantLock lock = new ReentrantLock(false);
  final Condition available = lock.newCondition();

  final int maxSize;
  final int maxBytes;

  final byte[][] elements;
  int count;
  int sizeInBytes;
  int writePos;
  int readPos;

  ByteBoundedQueue(int maxSize, int maxBytes) {
    this.elements = new byte[maxSize][];
    this.maxSize = maxSize;
    this.maxBytes = maxBytes;
  }

  /**
   * Returns true if the element could be added or false if it could not due to its size.
   */
  boolean offer(byte[] next) {
    lock.lock();
    try {
      if (count == elements.length) return false;
      if (sizeInBytes + next.length > maxBytes) return false;

      elements[writePos++] = next;

      if (writePos == elements.length) writePos = 0; // circle back to the front of the array

      count++;
      sizeInBytes += next.length;

      available.signal(); // alert any drainers
      return true;
    } finally {
      lock.unlock();
    }
  }

  /** Blocks for up to nanosTimeout for elements to appear. Then, consume as many as possible. */
  int drainTo(Consumer consumer, long nanosTimeout) {
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

  /** Clears the queue unconditionally and returns count of elements cleared. */
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

  int doDrain(Consumer consumer) {
    int drainedCount = 0;
    int drainedSizeInBytes = 0;
    while (drainedCount < count) {
      byte[] next = elements[readPos];

      if (next == null) break;
      if (consumer.accept(next)) {
        drainedCount++;
        drainedSizeInBytes += next.length;

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
