/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin2.reporter.metrics.micrometer;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;
import zipkin2.reporter.ReporterMetrics;

/**
 * Implementation of {@link ReporterMetrics} with Micrometer.
 */
public class MicrometerReporterMetrics implements ReporterMetrics {

  private static final String PREFIX = "zipkin.reporter.";

  final MeterRegistry meterRegistry;
  final Iterable<Tag> extraTags;

  final Counter messages;
  final Counter messageBytes;
  final Counter spans;
  final Counter spanBytes;
  final Counter spansDropped;
  final AtomicInteger queuedSpans;
  final AtomicInteger queuedBytes;

  /**
   * Creates a {@link MicrometerReporterMetrics} instance that registers all metrics to the given {@link MeterRegistry}.
   * Any other options use the default. To customize other options, use {@link #builder(MeterRegistry)} instead.
   *
   * @param meterRegistry all metrics will be registered to this registry
   */
  public static MicrometerReporterMetrics create(MeterRegistry meterRegistry) {
    return new Builder(meterRegistry).build();
  }

  /**
   * Like {@link #create(MeterRegistry)} but returns a builder where you can customize optional settings like extra tags.
   */
  public static Builder builder(MeterRegistry meterRegistry) {
    return new Builder(meterRegistry);
  }

  private MicrometerReporterMetrics(MeterRegistry meterRegistry, Tag... extraTags) {
    this.meterRegistry = meterRegistry;
    this.extraTags = Arrays.asList(extraTags);

    messages = Counter.builder(PREFIX + "messages.total")
      .description("Messages reported (or attempted to be reported)")
      .tags(this.extraTags).register(meterRegistry);
    messageBytes = Counter.builder(PREFIX + "messages")
      .description("Total bytes of messages reported")
      .baseUnit("bytes")
      .tags(this.extraTags).register(meterRegistry);
    spans = Counter.builder(PREFIX + "spans.total")
      .description("Spans reported")
      .tags(this.extraTags).register(meterRegistry);
    spanBytes = Counter.builder(PREFIX + "spans")
      .description("Total bytes of encoded spans reported")
      .baseUnit("bytes")
      .tags(this.extraTags).register(meterRegistry);
    spansDropped = Counter.builder(PREFIX + "spans.dropped")
      .description("Spans dropped (failed to report)")
      .tags(this.extraTags).register(meterRegistry);
    queuedSpans = new AtomicInteger();
    Gauge.builder(PREFIX + "queue.spans", queuedSpans, AtomicInteger::get)
      .description("Spans queued for reporting")
      .tags(this.extraTags).register(meterRegistry);
    queuedBytes = new AtomicInteger();
    Gauge.builder(PREFIX + "queue.bytes", queuedBytes, AtomicInteger::get)
      .description("Total size of all encoded spans queued for reporting")
      .baseUnit("bytes")
      .tags(this.extraTags).register(meterRegistry);
  }

  @Override
  public void incrementMessages() {
    messages.increment();
  }

  @Override
  public void incrementMessageBytes(int i) {
    messageBytes.increment(i);
  }

  @Override
  public void incrementMessagesDropped(Throwable cause) {
    Iterable<Tag> tags = Tags.concat(extraTags, "cause", cause.getClass().getSimpleName());
    meterRegistry.counter(PREFIX + "messages.dropped", tags).increment();
  }

  @Override
  public void incrementSpans(int i) {
    spans.increment(i);
  }

  @Override
  public void incrementSpanBytes(int i) {
    spanBytes.increment(i);
  }

  @Override
  public void incrementSpansDropped(int i) {
    spansDropped.increment(i);
  }

  @Override
  public void updateQueuedSpans(int i) {
    queuedSpans.set(i);
  }

  @Override
  public void updateQueuedBytes(int i) {
    queuedBytes.set(i);
  }

  public static final class Builder {
    final MeterRegistry meterRegistry;
    Tag[] extraTags = new Tag[0];

    Builder(MeterRegistry meterRegistry) {
      this.meterRegistry = meterRegistry;
    }

    /**
     * Additional tags to attach to all Zipkin Reporter metrics.
     */
    public Builder extraTags(Tag... extraTags) {
      this.extraTags = extraTags;
      return this;
    }

    public MicrometerReporterMetrics build() {
      return new MicrometerReporterMetrics(meterRegistry, extraTags);
    }
  }
}
