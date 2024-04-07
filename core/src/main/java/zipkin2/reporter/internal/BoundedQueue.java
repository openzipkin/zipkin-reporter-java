/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package zipkin2.reporter.internal;

/**
 * Multi-producer, multi-consumer queue that could be bounded by count or/and size.
 */
abstract class BoundedQueue<S> implements SpanWithSizeConsumer<S> {
    static <S> BoundedQueue<S> create(int maxSize, int maxBytes) {
        if (maxBytes > 0) {
            return new ByteBoundedQueue<S>(maxSize, maxBytes);
        } else {
            return new SizeBoundedQueue<S>(maxSize);
        }
    }

    /**
     * Max element's count of this bounded queue
     */
    abstract int maxSize();

    /**
     * Max element'size of this bounded queue
     */
    abstract int maxBytes();

    /**
     * Clear this bounded queue
     */
    abstract int clear();

    /**
     * Element's count of this bounded queue
     */
    abstract int count();

    /**
     * Element's size of this bounded queue
     */
    abstract int sizeInBytes();

    /**
     * Drains this bounded queue. Blocks for up to nanosTimeout for spans to appear.
     * Then, consume as many as possible.     
     */
    abstract int drainTo(SpanWithSizeConsumer<S> bundler, long remainingNanos);
}
