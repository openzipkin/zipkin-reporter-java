/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin2.reporter;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class InMemoryReporterMetricsTest {
  @Test void incrementMessagesDropped_sameExceptionTypeIsNotCountedMoreThanOnce() {
    InMemoryReporterMetrics inMemoryReporterMetrics = new InMemoryReporterMetrics();

    inMemoryReporterMetrics.incrementMessagesDropped(new RuntimeException());
    inMemoryReporterMetrics.incrementMessagesDropped(new RuntimeException());
    inMemoryReporterMetrics.incrementMessagesDropped(new IllegalStateException());

    assertThat(
      inMemoryReporterMetrics.messagesDroppedByCause().get(RuntimeException.class)).isEqualTo(2);
    assertThat(
      inMemoryReporterMetrics.messagesDroppedByCause().get(IllegalStateException.class)).isEqualTo(
      1);
    assertThat(inMemoryReporterMetrics.messagesDroppedByCause().size()).isEqualTo(2);
  }

  @Test void incrementMessagesDropped_multipleOccurrencesOfSameExceptionTypeAreCounted() {
    InMemoryReporterMetrics inMemoryReporterMetrics = new InMemoryReporterMetrics();

    inMemoryReporterMetrics.incrementMessagesDropped(new RuntimeException());
    inMemoryReporterMetrics.incrementMessagesDropped(new RuntimeException());
    inMemoryReporterMetrics.incrementMessagesDropped(new IllegalStateException());

    assertThat(inMemoryReporterMetrics.messagesDropped()).isEqualTo(3);
  }
}
