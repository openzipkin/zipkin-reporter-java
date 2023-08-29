# zipkin-otlp-reporter-brave
This allows you to send spans recorded by Brave 5.12+ with a OTLP reporter.

To start sending the spans you would need to use GRPC in the following way:

```xml
<dependency>
    <groupId>io.grpc</groupId>
    <artifactId>grpc-protobuf</artifactId>
    <version>${grpc.version}</version>
</dependency>
<dependency>
    <groupId>io.grpc</groupId>
    <artifactId>grpc-stub</artifactId>
    <version>${grpc.version}</version>
</dependency>
<dependency>
    <groupId>io.grpc</groupId>
    <artifactId>grpc-netty-shaded</artifactId>
    <version>${grpc.version}</version>
</dependency>
```

and then set up the `Reporter` and `SpanHandler` like this

```java

    SpanHandler otlpSpanHandler = OtlpSpanHandler
      .create(OtlpReporter.create(ManagedChannelBuilder.forAddress("localhost", 4317)
      // For demo purposes we disable security
      .usePlaintext()));

    Tracing tracing = Tracing.newBuilder()
      .currentTraceContext(braveCurrentTraceContext)
      .supportsJoin(false)
      .traceId128Bit(true)
      .sampler(Sampler.ALWAYS_SAMPLE)
      // Add the SpanHandler
      .addSpanHandler(otlpSpanHandler)
      .localServiceName("my-service")
      .build();
```
