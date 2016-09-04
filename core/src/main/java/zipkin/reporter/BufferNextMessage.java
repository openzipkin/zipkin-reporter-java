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

final class BufferNextMessage implements ByteBoundedQueue.Consumer {
  private final Sender sender;
  private final int maxBytes;
  private final long timeoutNanos;
  private final List<byte[]> buffer = new LinkedList<>();

  long deadlineNanoTime;
  int sizeInBytes;
  boolean bufferFull;

  BufferNextMessage(Sender sender, int maxBytes, long timeoutNanos) {
    this.sender = sender;
    this.maxBytes = maxBytes;
    this.timeoutNanos = timeoutNanos;
  }

  @Override
  public boolean accept(byte[] next) {
    buffer.add(next); // speculatively add to the buffer so we can size it
    int x = sender.messageSizeInBytes(buffer);
    int y = maxBytes;
    int includingNextVsMaxBytes = (x < y) ? -1 : ((x == y) ? 0 : 1);

    // If we can fit queued spans and the next into one message...
    if (includingNextVsMaxBytes <= 0) {
      sizeInBytes = x;

      if (includingNextVsMaxBytes == 0) {
        bufferFull = true;
      }
      return true;
    } else {
      buffer.remove(buffer.size() - 1);
      return false; // we couldn't fit the next message into this buffer
    }
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

  List<byte[]> drain() {
    if (buffer.isEmpty()) return Collections.emptyList();
    ArrayList<byte[]> result = new ArrayList<>(buffer);
    buffer.clear();
    sizeInBytes = 0;
    bufferFull = false;
    deadlineNanoTime = 0;
    return result;
  }

  int sizeInBytes() {
    return sizeInBytes;
  }
}
