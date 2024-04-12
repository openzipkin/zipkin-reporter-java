/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin2.reporter.beans.context;

import brave.handler.MutableSpan;
import brave.handler.SpanHandler;
import brave.propagation.TraceContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import zipkin2.junit5.ZipkinExtension;
import zipkin2.reporter.BytesMessageSender;
import zipkin2.reporter.beans.AsyncZipkinSpanHandlerFactoryBean;
import zipkin2.reporter.beans.OkHttpSenderFactoryBean;
import zipkin2.reporter.brave.AsyncZipkinSpanHandler;

import static org.assertj.core.api.Assertions.assertThat;

class ITAsyncZipkinSpanHandlerFactoryBean {
  @RegisterExtension public static ZipkinExtension zipkin = new ZipkinExtension();

  final AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();

  @AfterEach void close() {
    context.close();
  }

  @Configuration static class TestConfiguration {
    @Bean public OkHttpSenderFactoryBean sender() {
      OkHttpSenderFactoryBean okHttpSenderFactoryBean = new OkHttpSenderFactoryBean();
      okHttpSenderFactoryBean.setEndpoint(zipkin.httpUrl() + "/api/v2/spans");
      return okHttpSenderFactoryBean;
    }

    @Bean public AsyncZipkinSpanHandlerFactoryBean spanHandler(BytesMessageSender sender) {
      AsyncZipkinSpanHandlerFactoryBean asyncReporterFactoryBean =
        new AsyncZipkinSpanHandlerFactoryBean();
      asyncReporterFactoryBean.setSender(sender);
      asyncReporterFactoryBean.setMessageTimeout(0); // don't spawn a thread
      return asyncReporterFactoryBean;
    }
  }

  @Test void providesAsyncZipkinSpanHandler() {
    context.register(TestConfiguration.class);
    context.refresh();

    // Make sure the spring context was built correctly
    SpanHandler spanHandler = context.getBean(SpanHandler.class);
    assertThat(spanHandler).isInstanceOf(AsyncZipkinSpanHandler.class);

    // Create and handle a span
    TraceContext context = TraceContext.newBuilder().traceId(1).spanId(2).sampled(true).build();
    MutableSpan span = new MutableSpan(context, null);
    spanHandler.end(context, span, SpanHandler.Cause.FINISHED);

    // Prove the integrated setup works.
    ((AsyncZipkinSpanHandler) spanHandler).flush();
    assertThat(zipkin.getTraces()).hasSize(1);
  }
}
