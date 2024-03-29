/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin2.reporter.beans;

import brave.Tag;
import brave.handler.MutableSpan;
import brave.propagation.TraceContext;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import zipkin2.reporter.BytesEncoder;
import zipkin2.reporter.BytesMessageSender;
import zipkin2.reporter.Encoding;
import zipkin2.reporter.ReporterMetrics;
import zipkin2.reporter.brave.AsyncZipkinSpanHandler;
import zipkin2.reporter.brave.MutableSpanBytesEncoder;

import static org.assertj.core.api.Assertions.assertThat;

class AsyncZipkinSpanHandlerFactoryBeanTest {
  public static final Tag<Throwable> ERROR_TAG = new Tag<Throwable>("error") {
    @Override protected String parseValue(Throwable throwable, TraceContext traceContext) {
      return null;
    }
  };
  public static BytesMessageSender SENDER = new FakeSender();
  public static BytesMessageSender PROTO3_SENDER = new FakeSender() {
    @Override public Encoding encoding() {
      return Encoding.PROTO3;
    }
  };
  public static BytesEncoder<MutableSpan> CUSTOM_ENCODER = new BytesEncoder<MutableSpan>() {
    @Override public Encoding encoding() {
      return Encoding.PROTO3;
    }

    @Override public int sizeInBytes(MutableSpan input) {
      throw new UnsupportedOperationException();
    }

    @Override public byte[] encode(MutableSpan input) {
      throw new UnsupportedOperationException();
    }
  };
  public static ReporterMetrics METRICS = ReporterMetrics.NOOP_METRICS;

  XmlBeans context;

  @AfterEach void close() {
    if (context != null) context.close();
  }

  @Test void errorTag() {
    context = new XmlBeans(""
      + "<bean id=\"zipkinSpanHandler\" class=\"zipkin2.reporter.beans.AsyncZipkinSpanHandlerFactoryBean\">\n"
      + "  <property name=\"sender\">\n"
      + "    <util:constant static-field=\"" + getClass().getName() + ".SENDER\"/>\n"
      + "  </property>\n"
      + "  <property name=\"messageTimeout\" value=\"0\"/>\n" // disable thread for test
      + "  <property name=\"errorTag\">\n"
      + "    <util:constant static-field=\"" + getClass().getName() + ".ERROR_TAG\"/>\n"
      + "  </property>\n"
      + "</bean>\n"
    );

    assertThat(context.getBean("zipkinSpanHandler", AsyncZipkinSpanHandler.class))
      .extracting("spanReporter.encoder.delegate.writer.errorTag")
      .isSameAs(ERROR_TAG);
  }

  @Test void alwaysReportSpans() {
    context = new XmlBeans(""
      + "<bean id=\"zipkinSpanHandler\" class=\"zipkin2.reporter.beans.AsyncZipkinSpanHandlerFactoryBean\">\n"
      + "  <property name=\"sender\">\n"
      + "    <util:constant static-field=\"" + getClass().getName() + ".SENDER\"/>\n"
      + "  </property>\n"
      + "  <property name=\"messageTimeout\" value=\"0\"/>\n" // disable thread for test
      + "  <property name=\"alwaysReportSpans\" value=\"true\"/>\n"
      + "</bean>"
    );

    assertThat(context.getBean("zipkinSpanHandler", AsyncZipkinSpanHandler.class))
      .extracting("alwaysReportSpans")
      .isEqualTo(true);
  }

  // below copied from AsyncReporterFactoryBean
  @Test void sender() {
    context = new XmlBeans(""
      + "<bean id=\"zipkinSpanHandler\" class=\"zipkin2.reporter.beans.AsyncZipkinSpanHandlerFactoryBean\">\n"
      + "  <property name=\"sender\">\n"
      + "    <util:constant static-field=\"" + getClass().getName() + ".SENDER\"/>\n"
      + "  </property>\n"
      + "  <property name=\"messageTimeout\" value=\"0\"/>\n" // disable thread for test
      + "</bean>"
    );

    assertThat(context.getBean("zipkinSpanHandler", AsyncZipkinSpanHandler.class))
      .extracting("spanReporter.sender")
      .isEqualTo(SENDER);
  }

  @Test void sender_proto3() {
    context = new XmlBeans(""
      + "<bean id=\"zipkinSpanHandler\" class=\"zipkin2.reporter.beans.AsyncZipkinSpanHandlerFactoryBean\">\n"
      + "  <property name=\"sender\">\n"
      + "    <util:constant static-field=\"" + getClass().getName() + ".PROTO3_SENDER\"/>\n"
      + "  </property>\n"
      + "  <property name=\"messageTimeout\" value=\"0\"/>\n" // disable thread for test
      + "</bean>"
    );

    assertThat(context.getBean("zipkinSpanHandler", AsyncZipkinSpanHandler.class))
      .extracting("spanReporter.sender")
      .isEqualTo(PROTO3_SENDER);
  }

  @Test void metrics() {
    context = new XmlBeans(""
      + "<bean id=\"zipkinSpanHandler\" class=\"zipkin2.reporter.beans.AsyncZipkinSpanHandlerFactoryBean\">\n"
      + "  <property name=\"sender\">\n"
      + "    <util:constant static-field=\"" + getClass().getName() + ".SENDER\"/>\n"
      + "  </property>\n"
      + "  <property name=\"metrics\">\n"
      + "    <util:constant static-field=\"" + getClass().getName() + ".METRICS\"/>\n"
      + "  </property>\n"
      + "  <property name=\"messageTimeout\" value=\"0\"/>\n" // disable thread for test
      + "</bean>"
    );

    assertThat(context.getBean("zipkinSpanHandler", AsyncZipkinSpanHandler.class))
      .extracting("spanReporter.metrics")
      .isEqualTo(METRICS);
  }

  @Test void messageMaxBytes() {
    context = new XmlBeans(""
      + "<bean id=\"zipkinSpanHandler\" class=\"zipkin2.reporter.beans.AsyncZipkinSpanHandlerFactoryBean\">\n"
      + "  <property name=\"sender\">\n"
      + "    <util:constant static-field=\"" + getClass().getName() + ".SENDER\"/>\n"
      + "  </property>\n"
      + "  <property name=\"messageMaxBytes\" value=\"512\"/>\n"
      + "  <property name=\"messageTimeout\" value=\"0\"/>\n" // disable thread for test
      + "</bean>"
    );

    assertThat(context.getBean("zipkinSpanHandler", AsyncZipkinSpanHandler.class))
      .extracting("spanReporter.messageMaxBytes")
      .isEqualTo(512);
  }

  @Test void messageTimeout() {
    context = new XmlBeans(""
      + "<bean id=\"zipkinSpanHandler\" class=\"zipkin2.reporter.beans.AsyncZipkinSpanHandlerFactoryBean\">\n"
      + "  <property name=\"sender\">\n"
      + "    <util:constant static-field=\"" + getClass().getName() + ".SENDER\"/>\n"
      + "  </property>\n"
      + "  <property name=\"messageTimeout\" value=\"500\"/>\n"
      + "</bean>"
    );

    assertThat(context.getBean("zipkinSpanHandler", AsyncZipkinSpanHandler.class))
      .extracting("spanReporter.messageTimeoutNanos")
      .isEqualTo(TimeUnit.MILLISECONDS.toNanos(500));
  }

  @Test void closeTimeout() {
    context = new XmlBeans(""
      + "<bean id=\"zipkinSpanHandler\" class=\"zipkin2.reporter.beans.AsyncZipkinSpanHandlerFactoryBean\">\n"
      + "  <property name=\"sender\">\n"
      + "    <util:constant static-field=\"" + getClass().getName() + ".SENDER\"/>\n"
      + "  </property>\n"
      + "  <property name=\"closeTimeout\" value=\"500\"/>\n"
      + "  <property name=\"messageTimeout\" value=\"0\"/>\n" // disable thread for test
      + "</bean>"
    );

    assertThat(context.getBean("zipkinSpanHandler", AsyncZipkinSpanHandler.class))
      .extracting("spanReporter.closeTimeoutNanos")
      .isEqualTo(TimeUnit.MILLISECONDS.toNanos(500));
  }

  @Test void queuedMaxSpans() {
    context = new XmlBeans(""
      + "<bean id=\"zipkinSpanHandler\" class=\"zipkin2.reporter.beans.AsyncZipkinSpanHandlerFactoryBean\">\n"
      + "  <property name=\"sender\">\n"
      + "    <util:constant static-field=\"" + getClass().getName() + ".SENDER\"/>\n"
      + "  </property>\n"
      + "  <property name=\"queuedMaxSpans\" value=\"10\"/>\n"
      + "  <property name=\"messageTimeout\" value=\"0\"/>\n" // disable thread for test
      + "</bean>"
    );

    assertThat(context.getBean("zipkinSpanHandler", AsyncZipkinSpanHandler.class))
      .extracting("spanReporter.pending.maxSize")
      .isEqualTo(10);
  }

  @Test void queuedMaxBytes() {
    context = new XmlBeans(""
      + "<bean id=\"zipkinSpanHandler\" class=\"zipkin2.reporter.beans.AsyncZipkinSpanHandlerFactoryBean\">\n"
      + "  <property name=\"sender\">\n"
      + "    <util:constant static-field=\"" + getClass().getName() + ".SENDER\"/>\n"
      + "  </property>\n"
      + "  <property name=\"queuedMaxBytes\" value=\"512\"/>\n"
      + "  <property name=\"messageTimeout\" value=\"0\"/>\n" // disable thread for test
      + "</bean>"
    );

    assertThat(context.getBean("zipkinSpanHandler", AsyncZipkinSpanHandler.class))
      .extracting("spanReporter.pending.maxBytes")
      .isEqualTo(512);
  }

  @Test void encoder() {
    context = new XmlBeans(""
      + "<bean id=\"zipkinSpanHandler\" class=\"zipkin2.reporter.beans.AsyncZipkinSpanHandlerFactoryBean\">\n"
      + "  <property name=\"sender\">\n"
      + "    <util:constant static-field=\"" + getClass().getName() + ".PROTO3_SENDER\"/>\n"
      + "  </property>\n"
      + "  <property name=\"encoder\" value=\"PROTO3\"/>\n"
      + "  <property name=\"messageTimeout\" value=\"0\"/>\n" // disable thread for test
      + "</bean>"
    );

    assertThat(context.getBean("zipkinSpanHandler", AsyncZipkinSpanHandler.class))
      .extracting("spanReporter.encoder")
      .isEqualTo(MutableSpanBytesEncoder.PROTO3);
  }

  /** Ensures encoders not built-into zipkin-reporter can be used. */
  @Test void encoder_custom() {
    context = new XmlBeans(""
      + "<bean id=\"zipkinSpanHandler\" class=\"zipkin2.reporter.beans.AsyncZipkinSpanHandlerFactoryBean\">\n"
      + "  <property name=\"sender\">\n"
      + "    <util:constant static-field=\"" + getClass().getName() + ".PROTO3_SENDER\"/>\n"
      + "  </property>\n"
      + "  <property name=\"encoder\">\n"
      + "    <util:constant static-field=\"" + getClass().getName() + ".CUSTOM_ENCODER\"/>\n"
      + "  </property>\n"
      + "  <property name=\"messageTimeout\" value=\"0\"/>\n" // disable thread for test
      + "</bean>"
    );

    assertThat(context.getBean("zipkinSpanHandler", AsyncZipkinSpanHandler.class))
      .extracting("spanReporter.encoder")
      .isEqualTo(CUSTOM_ENCODER);
  }
}
