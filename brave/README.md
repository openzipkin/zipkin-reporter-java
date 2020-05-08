# zipkin-reporter-brave
This allows you to send spans recorded by Brave 5.12+ to a Zipkin reporter.

Ex.
```java
spanReporter = AsyncReporter.create(URLConnectionSender.create("http://localhost:9411/api/v2/spans"));
tracingBuilder.addSpanHandler(ZipkinSpanHandler.create(reporter));
```
