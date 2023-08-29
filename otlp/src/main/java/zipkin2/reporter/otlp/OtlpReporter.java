/*
 * Copyright 2016-2023 The OpenZipkin Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package zipkin2.reporter.otlp;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceResponse;
import io.opentelemetry.proto.collector.trace.v1.TraceServiceGrpc;
import io.opentelemetry.proto.collector.trace.v1.TraceServiceGrpc.TraceServiceStub;
import io.opentelemetry.proto.trace.v1.ResourceSpans;
import io.opentelemetry.proto.trace.v1.TracesData;
import zipkin2.reporter.Reporter;

/**
 * Asynchronous reporter of {@link ResourceSpans}.
 *
 * @since 2.16.5
 */
public final class OtlpReporter implements Reporter<TracesData>, Closeable {
  static final Logger logger = Logger.getLogger(OtlpReporter.class.getName());

  private final ManagedChannel channel;

  private final TraceServiceStub asyncStub;

  OtlpReporter(ManagedChannelBuilder<?> channelBuilder) {
    this.channel = channelBuilder.build();
    this.asyncStub = TraceServiceGrpc.newStub(this.channel);
  }

  /**
   * Creates a new instance of the {@link OtlpReporter}.
   *
   * @param channelBuilder channel builder
   * @return {@link Reporter} for {@link TracesData}
   * @since 2.16.5
   */
  public static Reporter<TracesData> create(ManagedChannelBuilder<?> channelBuilder) {
    return new OtlpReporter(channelBuilder);
  }

  @Override
  public void report(TracesData tracesData) {
    ExportTraceServiceRequest.Builder builder = ExportTraceServiceRequest.newBuilder();
    for (ResourceSpans spans : tracesData.getResourceSpansList()) {
      builder.addResourceSpans(spans);
    }
    this.asyncStub.export(builder.build(), new StreamObserver<ExportTraceServiceResponse>() {
      @Override
      public void onNext(ExportTraceServiceResponse exportTraceServiceResponse) {
        logger.log(Level.FINE, "Sent out spans to OTLP endpoint");
      }

      @Override
      public void onError(Throwable throwable) {
        logger.log(Level.WARNING, "Failed to send spans to OTLP endpoint", throwable);
      }

      @Override
      public void onCompleted() {
        logger.log(Level.FINE, "Completed sending of spans");
      }
    });
  }

  @Override
  public void close() throws IOException {
    this.channel.shutdown();
    try {
      this.channel.awaitTermination(1, TimeUnit.SECONDS);
    }
    catch (InterruptedException e) {
      logger.log(Level.WARNING, "Failed to close the OTLP channel", e);
    }
  }
}
