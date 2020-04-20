# zipkin-reporter-brave
This module contains integration between `zipkin2.reporter.Reporter` and
Brave's `FinishedSpanHandler`.

Ex.
```java
tracingBuilder.addFinishedSpanHandler(ZipkinFinishedSpanHandler.create(reporter));
```

This decouples Brave from this library and allows users to report to multiple
Zipkin destinations at the same time.


