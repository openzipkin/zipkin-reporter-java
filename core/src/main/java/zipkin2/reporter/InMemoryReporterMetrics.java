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

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public final class InMemoryReporterMetrics implements ReporterMetrics {
  enum MetricKey {
    messages,
    messageBytes,
    spans,
    spanBytes,
    spansDropped,
    spansPending,
    spanBytesPending;
  }

  private final ConcurrentHashMap<MetricKey, AtomicLong> metrics =
      new ConcurrentHashMap<>();
  private final ConcurrentHashMap<Class<? extends Throwable>, AtomicLong> messagesDropped =
      new ConcurrentHashMap<>();

  @Override public void incrementMessages() {
    increment(MetricKey.messages, 1);
  }

  public long messages() {
    return get(MetricKey.messages);
  }

  @Override public void incrementMessagesDropped(Throwable cause) {
    increment(messagesDropped, cause.getClass(), 1);
  }

  public Map<Class<? extends Throwable>, Long> messagesDroppedByCause() {
    Map<Class<? extends Throwable>, Long> result = new LinkedHashMap<>(messagesDropped.size());
    for (Map.Entry<Class<? extends Throwable>, AtomicLong> kv : messagesDropped.entrySet()) {
      result.put(kv.getKey(), kv.getValue().longValue());
    }
    return result;
  }

  public long messagesDropped() {
    long result = 0L;
    for (AtomicLong count : messagesDropped.values()) {
      result += count.longValue();
    }
    return result;
  }

  @Override public void incrementMessageBytes(int quantity) {
    increment(MetricKey.messageBytes, quantity);
  }

  public long messageBytes() {
    return get(MetricKey.messageBytes);
  }

  @Override public void incrementSpans(int quantity) {
    increment(MetricKey.spans, quantity);
  }

  public long spans() {
    return get(MetricKey.spans);
  }

  @Override public void incrementSpanBytes(int quantity) {
    increment(MetricKey.spanBytes, quantity);
  }

  public long spanBytes() {
    return get(MetricKey.spanBytes);
  }

  @Override
  public void incrementSpansDropped(int quantity) {
    increment(MetricKey.spansDropped, quantity);
  }

  public long spansDropped() {
    return get(MetricKey.spansDropped);
  }

  @Override public void updateQueuedSpans(int update) {
    update(MetricKey.spansPending, update);
  }

  public long queuedSpans() {
    return get(MetricKey.spansPending);
  }

  @Override public void updateQueuedBytes(int update) {
    update(MetricKey.spanBytesPending, update);
  }

  public long queuedBytes() {
    return get(MetricKey.spanBytesPending);
  }

  public void clear() {
    metrics.clear();
  }

  private long get(MetricKey key) {
    AtomicLong atomic = metrics.get(key);
    return atomic == null ? 0 : atomic.get();
  }

  private void increment(MetricKey key, int quantity) {
    increment(metrics, key, quantity);
  }

  static <K> void increment(ConcurrentHashMap<K, AtomicLong> metrics, K key, int quantity) {
    if (quantity == 0) return;
    while (true) {
      AtomicLong metric = metrics.get(key);
      if (metric == null) {
        metric = metrics.putIfAbsent(key, new AtomicLong(quantity));
        if (metric == null) return; // won race creating the entry 
      }

      while (true) {
        long oldValue = metric.get();
        long update = oldValue + quantity;
        if (metric.compareAndSet(oldValue, update)) return; // won race updating
      }
    }
  }

  private void update(MetricKey key, int update) {
    AtomicLong metric = metrics.get(key);
    if (metric == null) {
      metric = metrics.putIfAbsent(key, new AtomicLong(update));
      if (metric == null) return; // won race creating the entry
    }
    metric.set(update);
  }
}
