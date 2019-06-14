/*
 * Copyright 2016-2019 The OpenZipkin Authors
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

import java.util.concurrent.TimeUnit;
import org.junit.After;
import org.junit.Test;
import zipkin2.codec.Encoding;
import zipkin2.codec.SpanBytesEncoder;
import zipkin2.reporter.AsyncReporter;
import zipkin2.reporter.ReporterMetrics;
import zipkin2.reporter.Sender;
import zipkin2.reporter.urlconnection.URLConnectionSender;

import static org.assertj.core.api.Assertions.assertThat;

public class AsyncReporterFactoryBeanTest {

  public static Sender SENDER = URLConnectionSender.create("http://localhost:9411/api/v2/spans");
  public static Sender PROTO3_SENDER = URLConnectionSender.newBuilder()
      .endpoint("http://localhost:9411/api/v2/spans")
      .encoding(Encoding.PROTO3)
      .build();
  public static ReporterMetrics METRICS = ReporterMetrics.NOOP_METRICS;

  XmlBeans context;

  @After public void close() {
    if (context != null) context.close();
  }

  @Test public void sender() {
    context = new XmlBeans(""
        + "<bean id=\"asyncReporter\" class=\"zipkin2.reporter.beans.AsyncReporterFactoryBean\">\n"
        + "  <property name=\"sender\">\n"
        + "    <util:constant static-field=\"" + getClass().getName() + ".SENDER\"/>\n"
        + "  </property>\n"
        + "</bean>"
    );

    assertThat(context.getBean("asyncReporter", AsyncReporter.class))
        .extracting("sender")
        .containsExactly(SENDER);
  }

  @Test public void metrics() {
    context = new XmlBeans(""
        + "<bean id=\"asyncReporter\" class=\"zipkin2.reporter.beans.AsyncReporterFactoryBean\">\n"
        + "  <property name=\"sender\">\n"
        + "    <util:constant static-field=\"" + getClass().getName() + ".SENDER\"/>\n"
        + "  </property>\n"
        + "  <property name=\"metrics\">\n"
        + "    <util:constant static-field=\"" + getClass().getName() + ".METRICS\"/>\n"
        + "  </property>\n"
        + "</bean>"
    );

    assertThat(context.getBean("asyncReporter", AsyncReporter.class))
        .extracting("metrics")
        .containsExactly(METRICS);
  }

  @Test public void messageMaxBytes() {
    context = new XmlBeans(""
        + "<bean id=\"asyncReporter\" class=\"zipkin2.reporter.beans.AsyncReporterFactoryBean\">\n"
        + "  <property name=\"sender\">\n"
        + "    <util:constant static-field=\"" + getClass().getName() + ".SENDER\"/>\n"
        + "  </property>\n"
        + "  <property name=\"messageMaxBytes\" value=\"512\"/>\n"
        + "</bean>"
    );

    assertThat(context.getBean("asyncReporter", AsyncReporter.class))
        .extracting("messageMaxBytes")
        .containsExactly(512);
  }

  @Test public void messageTimeout() {
    context = new XmlBeans(""
        + "<bean id=\"asyncReporter\" class=\"zipkin2.reporter.beans.AsyncReporterFactoryBean\">\n"
        + "  <property name=\"sender\">\n"
        + "    <util:constant static-field=\"" + getClass().getName() + ".SENDER\"/>\n"
        + "  </property>\n"
        + "  <property name=\"messageTimeout\" value=\"500\"/>\n"
        + "</bean>"
    );

    assertThat(context.getBean("asyncReporter", AsyncReporter.class))
        .extracting("messageTimeoutNanos")
        .containsExactly(TimeUnit.MILLISECONDS.toNanos(500));
  }

  @Test public void closeTimeout() {
    context = new XmlBeans(""
        + "<bean id=\"asyncReporter\" class=\"zipkin2.reporter.beans.AsyncReporterFactoryBean\">\n"
        + "  <property name=\"sender\">\n"
        + "    <util:constant static-field=\"" + getClass().getName() + ".SENDER\"/>\n"
        + "  </property>\n"
        + "  <property name=\"closeTimeout\" value=\"500\"/>\n"
        + "</bean>"
    );

    assertThat(context.getBean("asyncReporter", AsyncReporter.class))
        .extracting("closeTimeoutNanos")
        .containsExactly(TimeUnit.MILLISECONDS.toNanos(500));
  }

  @Test public void queuedMaxSpans() {
    context = new XmlBeans(""
        + "<bean id=\"asyncReporter\" class=\"zipkin2.reporter.beans.AsyncReporterFactoryBean\">\n"
        + "  <property name=\"sender\">\n"
        + "    <util:constant static-field=\"" + getClass().getName() + ".SENDER\"/>\n"
        + "  </property>\n"
        + "  <property name=\"queuedMaxSpans\" value=\"10\"/>\n"
        + "</bean>"
    );

    assertThat(context.getBean("asyncReporter", AsyncReporter.class))
        .extracting("pending.maxSize")
        .containsExactly(10);
  }

  @Test public void queuedMaxBytes() {
    context = new XmlBeans(""
        + "<bean id=\"asyncReporter\" class=\"zipkin2.reporter.beans.AsyncReporterFactoryBean\">\n"
        + "  <property name=\"sender\">\n"
        + "    <util:constant static-field=\"" + getClass().getName() + ".SENDER\"/>\n"
        + "  </property>\n"
        + "  <property name=\"queuedMaxBytes\" value=\"512\"/>\n"
        + "</bean>"
    );

    assertThat(context.getBean("asyncReporter", AsyncReporter.class))
        .extracting("pending.maxBytes")
        .containsExactly(512);
  }

  @Test public void sender_proto3() {
    context = new XmlBeans(""
        + "<bean id=\"asyncReporter\" class=\"zipkin2.reporter.beans.AsyncReporterFactoryBean\">\n"
        + "  <property name=\"sender\">\n"
        + "    <util:constant static-field=\"" + getClass().getName() + ".PROTO3_SENDER\"/>\n"
        + "  </property>\n"
        + "</bean>"
    );

    assertThat(context.getBean("asyncReporter", AsyncReporter.class))
        .extracting("encoder")
        .containsExactly(SpanBytesEncoder.PROTO3);
  }

  @Test public void encoder() {
    context = new XmlBeans(""
        + "<bean id=\"asyncReporter\" class=\"zipkin2.reporter.beans.AsyncReporterFactoryBean\">\n"
        + "  <property name=\"sender\">\n"
        + "    <util:constant static-field=\"" + getClass().getName() + ".PROTO3_SENDER\"/>\n"
        + "  </property>\n"
        + "  <property name=\"encoder\" value=\"PROTO3\"/>\n"
        + "</bean>"
    );

    assertThat(context.getBean("asyncReporter", AsyncReporter.class))
        .extracting("encoder")
        .containsExactly(SpanBytesEncoder.PROTO3);
  }
}
