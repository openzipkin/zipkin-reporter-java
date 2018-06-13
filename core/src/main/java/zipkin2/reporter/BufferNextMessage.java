/*
 * Copyright 2016-2018 The OpenZipkin Authors
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
package zipkin2.reporter;

import java.util.ArrayList;
import java.util.Iterator;
import zipkin2.codec.Encoding;

/** Use of this type happens off the application's main thread. This type is not thread-safe */
abstract class BufferNextMessage<S> implements SpanWithSizeConsumer<S> {

  static <S> BufferNextMessage<S> create(Encoding encoding, int maxBytes, long timeoutNanos) {
    switch (encoding) {
      case JSON:
        return new BufferNextJsonMessage<>(maxBytes, timeoutNanos);
      case THRIFT:
        return new BufferNextThriftMessage<>(maxBytes, timeoutNanos);
      case PROTO3:
        return new BufferNextProto3Message<>(maxBytes, timeoutNanos);
    }
    throw new UnsupportedOperationException("encoding: " + encoding);
  }

  final int maxBytes;
  final long timeoutNanos;
  final ArrayList<S> spans = new ArrayList<>();
  final ArrayList<Integer> sizes = new ArrayList<>();

  long deadlineNanoTime;
  int messageSizeInBytes;
  boolean bufferFull;

  BufferNextMessage(int maxBytes, long timeoutNanos) {
    this.maxBytes = maxBytes;
    this.timeoutNanos = timeoutNanos;
  }

  abstract int messageSizeInBytes(int nextSizeInBytes);

  abstract void resetMessageSizeInBytes();

  static final class BufferNextJsonMessage<S> extends BufferNextMessage<S> {
    boolean hasAtLeastOneSpan;

    BufferNextJsonMessage(int maxBytes, long timeoutNanos) {
      super(maxBytes, timeoutNanos);
      messageSizeInBytes = 2;
      hasAtLeastOneSpan = false;
    }

    @Override
    int messageSizeInBytes(int nextSizeInBytes) {
      return messageSizeInBytes + nextSizeInBytes + (hasAtLeastOneSpan ? 1 : 0);
    }

    @Override
    void resetMessageSizeInBytes() {
      int length = sizes.size();
      hasAtLeastOneSpan = length > 0;
      if (length < 2) {
        messageSizeInBytes = 2;
        if (hasAtLeastOneSpan) messageSizeInBytes += sizes.get(0);
      } else {
        messageSizeInBytes = 2 + length - 1; // [] and commas
        for (int i = 0; i < length; i++) {
          messageSizeInBytes += sizes.get(i);
        }
      }
    }

    @Override
    void addSpanToBuffer(S next, int nextSizeInBytes) {
      super.addSpanToBuffer(next, nextSizeInBytes);
      hasAtLeastOneSpan = true;
    }
  }

  static final class BufferNextThriftMessage<S> extends BufferNextMessage<S> {

    BufferNextThriftMessage(int maxBytes, long timeoutNanos) {
      super(maxBytes, timeoutNanos);
      messageSizeInBytes = 5;
    }

    @Override
    int messageSizeInBytes(int nextSizeInBytes) {
      return messageSizeInBytes + nextSizeInBytes;
    }

    @Override
    void resetMessageSizeInBytes() {
      messageSizeInBytes = 5;
      for (int i = 0, length = sizes.size(); i < length; i++) {
        messageSizeInBytes += sizes.get(i);
      }
    }
  }

  static final class BufferNextProto3Message<S> extends BufferNextMessage<S> {
    BufferNextProto3Message(int maxBytes, long timeoutNanos) {
      super(maxBytes, timeoutNanos);
    }

    /** proto3 repeated fields are simply concatenated. there is no other overhead */
    @Override
    int messageSizeInBytes(int nextSizeInBytes) {
      return messageSizeInBytes += nextSizeInBytes;
    }

    @Override
    void resetMessageSizeInBytes() {
      messageSizeInBytes = 0;
      for (int i = 0, length = sizes.size(); i < length; i++) {
        messageSizeInBytes += sizes.get(i);
      }
    }
  }

  /** This is done inside a lock that holds up writers, so has to be fast. No encoding! */
  @Override
  public boolean offer(S next, int nextSizeInBytes) {
    int x = messageSizeInBytes(nextSizeInBytes);
    int y = maxBytes;
    int includingNextVsMaxBytes = (x < y) ? -1 : ((x == y) ? 0 : 1); // Integer.compare, but JRE 6

    if (includingNextVsMaxBytes > 0) return false; // can't fit the next message into this buffer

    addSpanToBuffer(next, nextSizeInBytes);
    messageSizeInBytes = x;

    if (includingNextVsMaxBytes == 0) bufferFull = true;
    return true;
  }

  void addSpanToBuffer(S next, int nextSizeInBytes) {
    spans.add(next);
    sizes.add(nextSizeInBytes);
  }

  long remainingNanos() {
    if (spans.isEmpty()) {
      deadlineNanoTime = System.nanoTime() + timeoutNanos;
    }
    return Math.max(deadlineNanoTime - System.nanoTime(), 0);
  }

  boolean isReady() {
    return bufferFull || remainingNanos() <= 0;
  }

  // this occurs off the application thread
  void drain(SpanWithSizeConsumer<S> consumer) {
    Iterator<S> spanIterator = spans.iterator();
    Iterator<Integer> sizeIterator = sizes.iterator();
    while (spanIterator.hasNext()) {
      if (consumer.offer(spanIterator.next(), sizeIterator.next())) {
        bufferFull = false;
        spanIterator.remove();
        sizeIterator.remove();
      }
    }

    resetMessageSizeInBytes();
    // regardless, reset the clock
    deadlineNanoTime = 0;
  }

  int count() {
    return spans.size();
  }

  int sizeInBytes() {
    return messageSizeInBytes;
  }
}
