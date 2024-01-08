# zipkin-reporter-brave
This allows you to send spans recorded by Brave 5.12+ to a Zipkin reporter.

If using Zipkin V2 JSON format, construct the span handler so that it writes Brave's data
directly as JSON:
```java
sender = URLConnectionSender.create("http://localhost:9411/api/v2/spans");
zipkinSpanHandler = AsyncZipkinSpanHandler.create(sender); // don't forget to close!
tracingBuilder.addSpanHandler(zipkinSpanHandler);
```

If you need to use a different format, you can pass a constructed reporter instead:
```java
reporter = AsyncReporter.builder(URLConnectionSender.create("http://localhost:9411/api/v1/spans"))
                        .build(SpanBytesEncoder.JSON_V1);
tracingBuilder.addSpanHandler(ZipkinSpanHandler.create(reporter));
```

*Note*: `ZipkinSpanHandler` requires an explicit dependency on
[io.zipkin.reporter2:zipkin-reporter](../core)
