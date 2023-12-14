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
package zipkin2.reporter.beans;

import brave.Tag;
import brave.propagation.TraceContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import zipkin2.reporter.Reporter;
import zipkin2.reporter.Sender;
import zipkin2.reporter.brave.ZipkinSpanHandler;
import zipkin2.reporter.urlconnection.URLConnectionSender;

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
