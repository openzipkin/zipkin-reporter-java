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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import static zipkin.internal.Util.checkNotNull;

public final class InMemoryReporterMetrics implements ReporterMetrics {

  private final ConcurrentHashMap<String, AtomicLong> metrics;
  private final String messages;
  private final String messageBytes;
  private final String messagesDropped;
  private final String spans;
  private final String spanBytes;
  private final String spansDropped;

  public InMemoryReporterMetrics() {
    this(new ConcurrentHashMap<String, AtomicLong>(), null);
  }

  InMemoryReporterMetrics(ConcurrentHashMap<String, AtomicLong> metrics, String transport) {
    this.metrics = metrics;
    this.messages = scope("messages", transport);
    this.messageBytes = scope("messageBytes", transport);
    this.messagesDropped = scope("messagesDropped", transport);
    this.spans = scope("spans", transport);
    this.spanBytes = scope("spanBytes", transport);
    this.spansDropped = scope("spansDropped", transport);
  }

  @Override public InMemoryReporterMetrics forTransport(String transportType) {
    return new InMemoryReporterMetrics(metrics, checkNotNull(transportType, "transportType"));
  }

  @Override public void incrementMessages() {
    increment(messages, 1);
  }

  public long messages() {
    return get(messages);
  }

  @Override public void incrementMessagesDropped() {
    increment(messagesDropped, 1);
  }

  public long messagesDropped() {
    return get(messagesDropped);
  }

  @Override public void incrementMessageBytes(int quantity) {
    increment(messageBytes, quantity);
  }

  public long messageBytes() {
    return get(messageBytes);
  }

  @Override public void incrementSpans(int quantity) {
    increment(spans, quantity);
  }

  public long spans() {
    return get(spans);
  }

  @Override public void incrementSpanBytes(int quantity) {
    increment(spanBytes, quantity);
  }

  public long spanBytes() {
    return get(spanBytes);
  }

  @Override
  public void incrementSpansDropped(int quantity) {
    increment(spansDropped, quantity);
  }

  public long spansDropped() {
    return get(spansDropped);
  }

  public void clear() {
    metrics.clear();
  }

  private long get(String key) {
    AtomicLong atomic = metrics.get(key);
    return atomic == null ? 0 : atomic.get();
  }

  private void increment(String key, int quantity) {
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

  static String scope(String key, String transport) {
    return key + (transport == null ? "" : "." + transport);
  }
}
