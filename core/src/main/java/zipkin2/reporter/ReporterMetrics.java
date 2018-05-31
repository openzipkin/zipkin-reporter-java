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

/**
 * Instrumented applications report spans over a transport such as Kafka to Zipkin Collectors.
 *
 * <p>Callbacks on this type are invoked by zipkin reporters to improve the visibility of the
 * system. A typical implementation will report metrics to a telemetry system for analysis and
 * reporting.
 *
 * <h3>Spans Reported vs Queryable Spans</h3>
 *
 * <p>A span in the context of reporting is <= span in the context of query. Instrumentation should
 * report a span only once except, but certain types of spans cross the network. For example, RPC
 * spans are reported at the client and the server separately.
 *
 * <h3>Key Relationships</h3>
 *
 * <p>The following relationships can be used to consider health of the tracing system.
 * <pre>
 * <ul>
 * <li>{@link #updateQueuedSpans Pending spans}. Alert when this increases over time as it could
 * lead to dropped spans.
 * <li>Dropped spans = Alert when this increases as it could indicate a queue backup.
 * <li>Successful Messages = {@link #incrementMessages() Accepted messages} -
 * {@link #incrementMessagesDropped Dropped messages}. Alert when this is more than amount of
 * messages received from collectors.</li>
 * </li>
 * </ul>
 * </pre>
 */
public interface ReporterMetrics {

  /**
   * Increments count of message attempts, which contain 1 or more spans. Ex POST requests or Kafka
   * messages sent.
   */
  void incrementMessages();

  /**
   * Increments count of messages that could not be sent. Ex host unavailable, or peer disconnect.
   */
  void incrementMessagesDropped(Throwable cause);

  /**
   * Increments the count of spans reported. When {@link AsyncReporter} is used, reported spans will
   * usually be a larger number than messages.
   */
  void incrementSpans(int quantity);

  /**
   * Increments the number of encoded span bytes reported.
   */
  void incrementSpanBytes(int quantity);

  /**
   * Increments the number of bytes containing encoded spans in a message.
   *
   * <p>This is a function of span bytes per message and overhead
   *
   * @see Sender#messageSizeInBytes
   */
  void incrementMessageBytes(int quantity);

  /**
   * Increments the count of spans dropped for any reason. For example, failure queueing or
   * sending.
   */
  void incrementSpansDropped(int quantity);

  /** Updates the count of spans pending, following a flush activity. */
  void updateQueuedSpans(int update);

  /** Updates the count of encoded span bytes pending, following a flush activity. */
  void updateQueuedBytes(int update);

  ReporterMetrics NOOP_METRICS = new ReporterMetrics() {

    @Override public void incrementMessages() {
    }

    @Override public void incrementMessagesDropped(Throwable cause) {
    }

    @Override public void incrementSpans(int quantity) {
    }

    @Override public void incrementSpanBytes(int quantity) {
    }

    @Override public void incrementMessageBytes(int quantity) {
    }

    @Override public void incrementSpansDropped(int quantity) {
    }

    @Override public void updateQueuedSpans(int update) {
    }

    @Override public void updateQueuedBytes(int update) {
    }

    @Override public String toString() {
      return "NoOpReporterMetrics";
    }
  };
}
