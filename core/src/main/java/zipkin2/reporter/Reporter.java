/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin2.reporter;

import zipkin2.Span;

/**
 * Spans are created in instrumentation, transported out-of-band, and eventually persisted.
 * Reporters sends spans (or encoded spans) recorded by instrumentation out of process.
 *
 * <S>Type of span to report, usually {@link Span}, but extracted for reporting other java
 * types like HTrace spans to zipkin, and to allow future Zipkin model types to be reported (ex.
 * zipkin2.Span).
 */
public interface Reporter<S> {
  Reporter<Span> NOOP = new Reporter<Span>() {
    @Override public void report(Span span) {
    }

    @Override public String toString() {
      return "NoopReporter{}";
    }
  };
  Reporter<Span> CONSOLE = new Reporter<Span>() {
    @Override public void report(Span span) {
      System.out.println(span.toString());
    }

    @Override public String toString() {
      return "ConsoleReporter{}";
    }
  };

  /**
   * Schedules the span to be sent onto the transport.
   *
   * @param span Span, should not be <code>null</code>.
   */
  void report(S span);
}
