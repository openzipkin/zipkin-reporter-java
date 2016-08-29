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

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import zipkin.reporter.Sender.MessageEncoding;

final class BufferNextMessage<B> implements ByteBoundedQueue.Consumer<B> {
  private final MessageEncoding encoding;
  private final int maxBytes;
  private final long timeoutNanos;
  private final List<B> buffer = new LinkedList<>();

  long deadlineNanoTime;
  int bufferSizeInBytes;
  boolean bufferFull;

  BufferNextMessage(MessageEncoding encoding, int maxBytes, long timeoutNanos) {
    this.encoding = encoding;
    this.maxBytes = maxBytes;
    this.timeoutNanos = timeoutNanos;
  }

  @Override
  public boolean accept(B next, int nextSizeInBytes) {
    int x = encoding.overheadInBytes(buffer.size() + 1) + bufferSizeInBytes + nextSizeInBytes;
    int y = maxBytes;
    int includingNextVsMaxBytes = (x < y) ? -1 : ((x == y) ? 0 : 1);

    // If we can fit queued spans and the next into one message...
    if (includingNextVsMaxBytes <= 0) {
      buffer.add(next);
      bufferSizeInBytes += nextSizeInBytes;

      // If there's still room, accept more.
      if (includingNextVsMaxBytes < 0) return true;
    }
    bufferFull = true;
    return false; // Either we've reached exact message size or cannot consume next
  }

  long remainingNanos() {
    if (buffer.isEmpty()) {
      deadlineNanoTime = System.nanoTime() + timeoutNanos;
    }
    return Math.max(deadlineNanoTime - System.nanoTime(), 0);
  }

  boolean isReady() {
    return bufferFull || remainingNanos() <= 0;
  }

  List<B> drain() {
    if (buffer.isEmpty()) return Collections.emptyList();
    ArrayList<B> result = new ArrayList<>(buffer);
    buffer.clear();
    bufferSizeInBytes = 0;
    bufferFull = false;
    deadlineNanoTime = 0;
    return result;
  }

  int sizeInBytes() {
    return encoding.overheadInBytes(buffer.size()) + bufferSizeInBytes;
  }
}