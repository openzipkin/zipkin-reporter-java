# zipkin-reporter-brave
This allows you to send spans recorded by Brave to a Zipkin reporter.

Ex.
```java
spanReporter = AsyncReporter.create(URLConnectionSender.create("http://localhost:9411/api/v2/spans"));
tracingBuilder.addFinishedSpanHandler(ZipkinSpanHandler.create(reporter));
```



