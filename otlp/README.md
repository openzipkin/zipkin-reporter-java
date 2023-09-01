# zipkin-reporter-brave-otlp
This allows you to send spans recorded by Brave 5.12+ to a OTLP reporter over HTTP with PROTO encoding.

You can use any sender but remember to use the PROTO3 encoding. You can use the async reporting mechanism as shown below.

```java
okHttpSender = OkHttpSender.newBuilder()
  .encoding(Encoding.PROTO3)
  .endpoint("http://localhost:4318/v1/traces")
  .build();
spanHandler = AsyncOtlpSpanHandler.create(okHttpSender); // don't forget to close!
tracingBuilder.addSpanHandler(spanHandler);
```

There's also an option to use a synchronous reporter (which is less effective than the asynchronous version).

```java
okHttpSender = OkHttpSender.newBuilder()
  .encoding(Encoding.PROTO3)
  .endpoint("http://localhost:4318/v1/traces")
  .build();
reporter = SyncOtlpReporter.create(okHttpSender);
spanHandler = OtlpSpanHandler.create(reporter);
```
