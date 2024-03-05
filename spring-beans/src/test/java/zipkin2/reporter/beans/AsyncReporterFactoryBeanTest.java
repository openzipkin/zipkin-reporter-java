/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin2.reporter.beans;

import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import zipkin2.Span;
import zipkin2.reporter.AsyncReporter;
import zipkin2.reporter.BytesEncoder;
import zipkin2.reporter.BytesMessageSender;
import zipkin2.reporter.Encoding;
import zipkin2.reporter.ReporterMetrics;
import zipkin2.reporter.SpanBytesEncoder;

import static org.assertj.core.api.Assertions.assertThat;

class AsyncReporterFactoryBeanTest {
  public static BytesMessageSender SENDER = new FakeSender();
  public static BytesMessageSender PROTO3_SENDER = new FakeSender() {
    @Override public Encoding encoding() {
      return Encoding.PROTO3;
    }
  };
  public static BytesEncoder<Span> CUSTOM_ENCODER = new BytesEncoder<Span>() {
    @Override public Encoding encoding() {
      return Encoding.PROTO3;
    }

    @Override public int sizeInBytes(Span input) {
      throw new UnsupportedOperationException();
    }

    @Override public byte[] encode(Span input) {
      throw new UnsupportedOperationException();
    }
  };
  public static ReporterMetrics METRICS = ReporterMetrics.NOOP_METRICS;

  XmlBeans context;

  @AfterEach void close() {
    if (context != null) context.close();
  }

  @Test void sender() {
    context = new XmlBeans(""
      + "<bean id=\"asyncReporter\" class=\"zipkin2.reporter.beans.AsyncReporterFactoryBean\">\n"
      + "  <property name=\"sender\">\n"
      + "    <util:constant static-field=\"" + getClass().getName() + ".SENDER\"/>\n"
      + "  </property>\n"
      + "  <property name=\"messageTimeout\" value=\"0\"/>\n" // disable thread for test
      + "</bean>"
    );

    assertThat(context.getBean("asyncReporter", AsyncReporter.class))
      .extracting("delegate.sender")
      .isEqualTo(SENDER);
  }

  @Test void metrics() {
    context = new XmlBeans(""
      + "<bean id=\"asyncReporter\" class=\"zipkin2.reporter.beans.AsyncReporterFactoryBean\">\n"
      + "  <property name=\"sender\">\n"
      + "    <util:constant static-field=\"" + getClass().getName() + ".SENDER\"/>\n"
      + "  </property>\n"
      + "  <property name=\"metrics\">\n"
      + "    <util:constant static-field=\"" + getClass().getName() + ".METRICS\"/>\n"
      + "  </property>\n"
      + "  <property name=\"messageTimeout\" value=\"0\"/>\n" // disable thread for test
      + "</bean>"
    );

    assertThat(context.getBean("asyncReporter", AsyncReporter.class))
      .extracting("delegate.metrics")
      .isEqualTo(METRICS);
  }

  @Test void messageMaxBytes() {
    context = new XmlBeans(""
      + "<bean id=\"asyncReporter\" class=\"zipkin2.reporter.beans.AsyncReporterFactoryBean\">\n"
      + "  <property name=\"sender\">\n"
      + "    <util:constant static-field=\"" + getClass().getName() + ".SENDER\"/>\n"
      + "  </property>\n"
      + "  <property name=\"messageMaxBytes\" value=\"512\"/>\n"
      + "  <property name=\"messageTimeout\" value=\"0\"/>\n" // disable thread for test
      + "</bean>"
    );

    assertThat(context.getBean("asyncReporter", AsyncReporter.class))
      .extracting("delegate.messageMaxBytes")
      .isEqualTo(512);
  }

  @Test void messageTimeout() {
    context = new XmlBeans(""
      + "<bean id=\"asyncReporter\" class=\"zipkin2.reporter.beans.AsyncReporterFactoryBean\">\n"
      + "  <property name=\"sender\">\n"
      + "    <util:constant static-field=\"" + getClass().getName() + ".SENDER\"/>\n"
      + "  </property>\n"
      + "  <property name=\"messageTimeout\" value=\"500\"/>\n"
      + "</bean>"
    );

    assertThat(context.getBean("asyncReporter", AsyncReporter.class))
      .extracting("delegate.messageTimeoutNanos")
      .isEqualTo(TimeUnit.MILLISECONDS.toNanos(500));
  }

  @Test void closeTimeout() {
    context = new XmlBeans(""
      + "<bean id=\"asyncReporter\" class=\"zipkin2.reporter.beans.AsyncReporterFactoryBean\">\n"
      + "  <property name=\"sender\">\n"
      + "    <util:constant static-field=\"" + getClass().getName() + ".SENDER\"/>\n"
      + "  </property>\n"
      + "  <property name=\"closeTimeout\" value=\"500\"/>\n"
      + "  <property name=\"messageTimeout\" value=\"0\"/>\n" // disable thread for test
      + "</bean>"
    );

    assertThat(context.getBean("asyncReporter", AsyncReporter.class))
      .extracting("delegate.closeTimeoutNanos")
      .isEqualTo(TimeUnit.MILLISECONDS.toNanos(500));
  }

  @Test void queuedMaxSpans() {
    context = new XmlBeans(""
      + "<bean id=\"asyncReporter\" class=\"zipkin2.reporter.beans.AsyncReporterFactoryBean\">\n"
      + "  <property name=\"sender\">\n"
      + "    <util:constant static-field=\"" + getClass().getName() + ".SENDER\"/>\n"
      + "  </property>\n"
      + "  <property name=\"queuedMaxSpans\" value=\"10\"/>\n"
      + "  <property name=\"messageTimeout\" value=\"0\"/>\n" // disable thread for test
      + "</bean>"
    );

    assertThat(context.getBean("asyncReporter", AsyncReporter.class))
      .extracting("delegate.pending.maxSize")
      .isEqualTo(10);
  }

  @Test void queuedMaxBytes() {
    context = new XmlBeans(""
      + "<bean id=\"asyncReporter\" class=\"zipkin2.reporter.beans.AsyncReporterFactoryBean\">\n"
      + "  <property name=\"sender\">\n"
      + "    <util:constant static-field=\"" + getClass().getName() + ".SENDER\"/>\n"
      + "  </property>\n"
      + "  <property name=\"queuedMaxBytes\" value=\"512\"/>\n"
      + "  <property name=\"messageTimeout\" value=\"0\"/>\n" // disable thread for test
      + "</bean>"
    );

    assertThat(context.getBean("asyncReporter", AsyncReporter.class))
      .extracting("delegate.pending.maxBytes")
      .isEqualTo(512);
  }

  @Test void sender_proto3() {
    context = new XmlBeans(""
      + "<bean id=\"asyncReporter\" class=\"zipkin2.reporter.beans.AsyncReporterFactoryBean\">\n"
      + "  <property name=\"sender\">\n"
      + "    <util:constant static-field=\"" + getClass().getName() + ".PROTO3_SENDER\"/>\n"
      + "  </property>\n"
      + "  <property name=\"messageTimeout\" value=\"0\"/>\n" // disable thread for test
      + "</bean>"
    );

    assertThat(context.getBean("asyncReporter", AsyncReporter.class))
      .extracting("delegate.encoder.delegate")
      .isEqualTo(SpanBytesEncoder.PROTO3);
  }

  @Test void encoder() {
    context = new XmlBeans(""
      + "<bean id=\"asyncReporter\" class=\"zipkin2.reporter.beans.AsyncReporterFactoryBean\">\n"
      + "  <property name=\"sender\">\n"
      + "    <util:constant static-field=\"" + getClass().getName() + ".PROTO3_SENDER\"/>\n"
      + "  </property>\n"
      + "  <property name=\"encoder\" value=\"PROTO3\"/>\n"
      + "  <property name=\"messageTimeout\" value=\"0\"/>\n" // disable thread for test
      + "</bean>"
    );

    assertThat(context.getBean("asyncReporter", AsyncReporter.class))
      .extracting("delegate.encoder.delegate")
      .isEqualTo(SpanBytesEncoder.PROTO3);
  }

  @Test void encoder_custom() {
    context = new XmlBeans(""
      + "<bean id=\"asyncReporter\" class=\"zipkin2.reporter.beans.AsyncReporterFactoryBean\">\n"
      + "  <property name=\"sender\">\n"
      + "    <util:constant static-field=\"" + getClass().getName() + ".PROTO3_SENDER\"/>\n"
      + "  </property>\n"
      + "  <property name=\"encoder\">\n"
      + "    <util:constant static-field=\"" + getClass().getName() + ".CUSTOM_ENCODER\"/>\n"
      + "  </property>\n"
      + "  <property name=\"messageTimeout\" value=\"0\"/>\n" // disable thread for test
      + "</bean>"
    );

    assertThat(context.getBean("asyncReporter", AsyncReporter.class))
      .extracting("delegate.encoder.delegate")
      .isEqualTo(CUSTOM_ENCODER);
  }
}
