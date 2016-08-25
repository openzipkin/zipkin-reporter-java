[![Gitter chat](http://img.shields.io/badge/gitter-join%20chat%20%E2%86%92-brightgreen.svg)](https://gitter.im/openzipkin/zipkin) [![Build Status](https://travis-ci.org/openzipkin/zipkin-reporter-java.svg?branch=master)](https://travis-ci.org/openzipkin/zipkin-reporter-java) [![Download](https://api.bintray.com/packages/openzipkin/maven/zipkin-reporter-java/images/download.svg) ](https://bintray.com/openzipkin/maven/zipkin-reporter-java/_latestVersion)

# zipkin-reporter-java
Shared library for reporting zipkin spans onto transports including http and kafka. Requires JRE 6 or later.

# Usage
These components can be called when spans have been recorded and ready to send to zipkin.

## SpanEncoder
The span encoder is a specialized form of Zipkin's Codec, which only deals with encoding one span.

### Sender
The sender component handles the last step of sending a list of encoded spans onto a transport.
This involves I/O, so you can call `Sender.check()` to check its health on a given frequency. 

```java
class CustomReporter implements Flushable {

  --snip--
  URLConnectionSender sender = URLConnectionSender.create("http://localhost:9411/api/v1/spans");

  Callback callback = new IncrementSpanMetricsCallback(metrics);

  // Is the connection healthy?
  public boolean ok() {
    return sender.check().ok;
  }

  public void report(Span span) {
    pending.add(Encoder.THRIFT_BYTES.encode(span));
  }

  @Override
  public void flush() {
    if (pending.isEmpty()) return;
    List<byte[]> drained = new ArrayList<byte[]>(pending.size());
    pending.drainTo(drained);
    if (drained.isEmpty()) return;

    sender.sendSpans(drained, callback);
  }
```

## Json Encoding
By default, components use thrift encoding, as it is the most compatible
and efficient. However, json is readable and helpful during debugging.

Here's an example of how to switch to json encoding:

```java
sender = URLConnectionSender.builder()
                            .messageEncoder(MessageEncoder.JSON_BYTES)
                            .endpoint("http://localhost:9411/api/v1/spans")
                            .build();
```
