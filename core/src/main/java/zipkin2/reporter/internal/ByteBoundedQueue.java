/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin2.reporter.internal;

import zipkin2.reporter.ReporterMetrics;

/**
 * Multi-producer, multi-consumer queue that is bounded by both count and size.
 *
 * <p>This is similar to {@link java.util.concurrent.ArrayBlockingQueue} in implementation.
 */
class ByteBoundedQueue<S> extends BoundedQueue<S> {

  final int maxBytes;
  final int[] sizesInBytes;
  int sizeInBytes;

  ByteBoundedQueue(ReporterMetrics metrics, int maxSize, int maxBytes) {
    super(metrics, maxSize);
    this.sizesInBytes = new int[maxSize];
    this.maxBytes = maxBytes;
  }

  @Override boolean doOffer(S next, int nextSizeInBytes) {
    if (sizeInBytes + nextSizeInBytes > maxBytes) return false;

    int writePos = this.writePos;
    if (!super.doOffer(next, nextSizeInBytes)) {
      return false;
    }
    sizesInBytes[writePos] = nextSizeInBytes;
    sizeInBytes += nextSizeInBytes;
    return true;
  }

  @Override int doClear() {
    sizeInBytes = 0;
    return super.doClear();
  }

  @Override long doDrain(SpanWithSizeConsumer<S> consumer) {
    long drained = super.doDrain(consumer);
    int drainedSizeInBytes = (int) (drained >>> 32);
    sizeInBytes -= drainedSizeInBytes;
    metrics.updateQueuedBytes(sizeInBytes);
    return drained;
  }

  @Override int getSizeInBytes(int pos) {
    return sizesInBytes[pos];
  }
}
