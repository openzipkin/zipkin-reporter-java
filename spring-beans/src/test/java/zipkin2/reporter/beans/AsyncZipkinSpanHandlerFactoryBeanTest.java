/*
 * Copyright 2016-2020 The OpenZipkin Authors
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
package zipkin2.reporter.beans;

import brave.Tag;
import brave.propagation.TraceContext;
import java.util.concurrent.TimeUnit;
import org.junit.After;
import org.junit.Test;
import zipkin2.reporter.ReporterMetrics;
import zipkin2.reporter.Sender;
import zipkin2.reporter.brave.AsyncZipkinSpanHandler;
import zipkin2.reporter.brave.ZipkinSpanHandler;

import static org.assertj.core.api.Assertions.assertThat;

public class AsyncZipkinSpanHandlerFactoryBeanTest {
  public static final Tag<Throwable> ERROR_TAG = new Tag<Throwable>("error") {
    @Override protected String parseValue(Throwable throwable, TraceContext traceContext) {
      return null;
    }
  };
  public static Sender SENDER = new FakeSender();
  public static ReporterMetrics METRICS = ReporterMetrics.NOOP_METRICS;

  XmlBeans context;

  @After public void close() {
    if (context != null) context.close();
  }

  @Test public void errorTag() {
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

  @Test public void alwaysReportSpans() {
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
  @Test public void sender() {
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

  @Test public void metrics() {
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

  @Test public void messageMaxBytes() {
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

  @Test public void messageTimeout() {
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

  @Test public void closeTimeout() {
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

  @Test public void queuedMaxSpans() {
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

  @Test public void queuedMaxBytes() {
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
}
