/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin2.reporter.metrics.micrometer;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class MicrometerReporterMetricsTest {
  MeterRegistry meterRegistry = new SimpleMeterRegistry();
  MicrometerReporterMetrics reporterMetrics = MicrometerReporterMetrics.create(meterRegistry);

  @Test void expectedMetricsRegistered() {
    assertThat(meterRegistry.getMeters())
      .extracting(Meter::getId).extracting(Meter.Id::getName)
      .containsExactlyInAnyOrder(
        "zipkin.reporter.messages.total",
        "zipkin.reporter.messages",
        "zipkin.reporter.spans.total",
        "zipkin.reporter.spans",
        "zipkin.reporter.spans.dropped",
        "zipkin.reporter.queue.spans",
        "zipkin.reporter.queue.bytes"
      );
  }

  @Test void incrementMessagesDropped_sameExceptionTypeIsNotTaggedMoreThanOnce() {
    reporterMetrics.incrementMessagesDropped(new RuntimeException("boo"));
    reporterMetrics.incrementMessagesDropped(new RuntimeException("shh"));
    reporterMetrics.incrementMessagesDropped(new IllegalStateException());

    assertThat(meterRegistry.get("zipkin.reporter.messages.dropped").counters())
      .hasSize(2); // two distinct meters for each cause
    assertThat(meterRegistry.get("zipkin.reporter.messages.dropped").tag("cause", RuntimeException.class.getSimpleName()).counter().count()).isEqualTo(2);
    assertThat(meterRegistry.get("zipkin.reporter.messages.dropped").tag("cause", IllegalStateException.class.getSimpleName()).counter().count()).isEqualTo(1);
    double messagesDroppedTotal = meterRegistry.get("zipkin.reporter.messages.dropped").counters().stream().mapToDouble(Counter::count).sum();
    assertThat(messagesDroppedTotal).isEqualTo(3); // 3 total messages dropped
  }

  @Test void gaugesSurviveGc() {
    reporterMetrics.updateQueuedBytes(53);
    reporterMetrics.updateQueuedSpans(2);

    System.gc();

    assertThat(meterRegistry.get("zipkin.reporter.queue.bytes").gauge().value()).isEqualTo(53);
    assertThat(meterRegistry.get("zipkin.reporter.queue.spans").gauge().value()).isEqualTo(2);
  }
}
