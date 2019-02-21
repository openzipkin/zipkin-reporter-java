Zipkin Reporter Metrics by Micrometer
=====

This module provides an implementation of Zipkin Reporter's `ReporterMetrics` for [Micrometer](https://micrometer.io).

Usage
---

Pass an instance of `MicrometerReporterMetrics` to the `AsyncReporter` builder like below.

```java
reporter = AsyncReporter.builder(sender).metrics(MicrometerReporterMetrics.create(meterRegistry)).build();
```

### Extra tags

You may optionally configure Micrometer tags to add to all reporter metrics created using the builder.

For example, if an equivalent common tag is not already configured, you will want the Zipkin service name to be tagged so reporter metrics can be differentiated by service.

```java
MicrometerReporterMetrics reporterMetrics = MicrometerReporterMetrics.builder(meterRegistry).extraTags(reporterTags).build();
```
