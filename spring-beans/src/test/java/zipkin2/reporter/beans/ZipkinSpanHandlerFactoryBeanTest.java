/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin2.reporter.beans;

import brave.Tag;
import brave.propagation.TraceContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import zipkin2.reporter.Reporter;
import zipkin2.reporter.brave.ZipkinSpanHandler;

import static org.assertj.core.api.Assertions.assertThat;

class ZipkinSpanHandlerFactoryBeanTest {
  public static final Tag<Throwable> ERROR_TAG = new Tag<Throwable>("error") {
    @Override protected String parseValue(Throwable throwable, TraceContext traceContext) {
      return null;
    }
  };

  XmlBeans context;

  @AfterEach void close() {
    if (context != null) context.close();
  }

  @Test void spanReporter() {
    context = new XmlBeans(""
        + "<bean id=\"zipkinSpanHandler\" class=\"zipkin2.reporter.beans.ZipkinSpanHandlerFactoryBean\">\n"
        + "  <property name=\"spanReporter\">\n"
        + "    <util:constant static-field=\"zipkin2.reporter.Reporter.CONSOLE\"/>\n"
        + "  </property>\n"
        + "</bean>"
    );

    assertThat(context.getBean("zipkinSpanHandler", ZipkinSpanHandler.class))
        .extracting("spanReporter.delegate")
        .isEqualTo(Reporter.CONSOLE);
  }

  @Test void errorTag() {
    context = new XmlBeans(""
        + "<bean id=\"zipkinSpanHandler\" class=\"zipkin2.reporter.beans.ZipkinSpanHandlerFactoryBean\">\n"
        + "  <property name=\"spanReporter\">\n"
        + "    <util:constant static-field=\"zipkin2.reporter.Reporter.CONSOLE\"/>\n"
        + "  </property>\n"
        + "  <property name=\"errorTag\">\n"
        + "    <util:constant static-field=\"" + getClass().getName() + ".ERROR_TAG\"/>\n"
        + "  </property>\n"
        + "</bean>"
    );

    assertThat(context.getBean("zipkinSpanHandler", ZipkinSpanHandler.class))
        .extracting("spanReporter.errorTag")
        .isSameAs(ERROR_TAG);
  }

  @Test void alwaysReportSpans() {
    context = new XmlBeans(""
        + "<bean id=\"zipkinSpanHandler\" class=\"zipkin2.reporter.beans.ZipkinSpanHandlerFactoryBean\">\n"
        + "  <property name=\"spanReporter\">\n"
        + "    <util:constant static-field=\"zipkin2.reporter.Reporter.CONSOLE\"/>\n"
        + "  </property>\n"
        + "  <property name=\"alwaysReportSpans\" value=\"true\"/>\n"
        + "</bean>"
    );

    assertThat(context.getBean("zipkinSpanHandler", ZipkinSpanHandler.class))
        .extracting("alwaysReportSpans")
        .isEqualTo(true);
  }
}
