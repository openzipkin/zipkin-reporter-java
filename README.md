[![Gitter chat](http://img.shields.io/badge/gitter-join%20chat%20%E2%86%92-brightgreen.svg)](https://gitter.im/openzipkin/zipkin) [![Build Status](https://travis-ci.org/openzipkin/zipkin-reporter-java.svg?branch=master)](https://travis-ci.org/openzipkin/zipkin-reporter-java) [![Download](https://api.bintray.com/packages/openzipkin/maven/zipkin-reporter-java/images/download.svg) ](https://bintray.com/openzipkin/maven/zipkin-reporter-java/_latestVersion)

# zipkin-reporter-java
Shared library for reporting zipkin spans on transports including http and kafka.

# Usage
These components can be called when spans have been recorded and ready to send to zipkin.

For example, you may have a class called Recorder, which flushes on an interval. The reporter
component handles the last step.

```java
class Recorder implements Flushable {

  --snip--
  URLConnectionReporter reporter = URLConnectionReporter.builder()
                                                        .endpoint("http://localhost:9411/api/v1/spans")
                                                        .build();

  Callback callback = new IncrementSpanMetricsCallback(metrics);

  @Override
  public void flush() {
    if (pending.isEmpty()) return;
    List<Span> drained = new ArrayList<Span>(pending.size());
    pending.drainTo(drained);
    if (drained.isEmpty()) return;

    reporter.accept(drained, callback);
  }
```
