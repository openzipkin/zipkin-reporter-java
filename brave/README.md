# zipkin-reporter-brave
This allows you to send spans recorded by Brave 5.6+ to a Zipkin reporter.

Ex.
```java
spanReporter = AsyncReporter.create(URLConnectionSender.create("http://localhost:9411/api/v2/spans"));
tracingBuilder.addFinishedSpanHandler(ZipkinSpanHandler.create(reporter));
```

*Note*: The minimum version of Brave is 5.6. If you are using a version of Brave before 5.12, you
will also need to set the `spanReporter` to `Reporter.NOOP`. Otherwise, you will see a log
message for each span.

Ex.
```java
tracingBuilder.spanReporter(Reporter.NOOP);
```
