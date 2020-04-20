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

import brave.ErrorParser;
import org.junit.After;
import org.junit.Test;
import zipkin2.reporter.Reporter;
import zipkin2.reporter.brave.ZipkinSpanHandler;

import static org.assertj.core.api.Assertions.assertThat;

public class ZipkinSpanHandlerFactoryBeanTest {
  public static final ErrorParser ERROR_PARSER = new ErrorParser();

  XmlBeans context;

  @After public void close() {
    if (context != null) context.close();
  }

  @Test public void spanReporter() {
    context = new XmlBeans(""
      + "<bean id=\"zipkinSpanHandler\" class=\"zipkin2.reporter.beans.ZipkinSpanHandlerFactoryBean\">\n"
      + "  <property name=\"spanReporter\">\n"
      + "    <util:constant static-field=\"zipkin2.reporter.Reporter.CONSOLE\"/>\n"
      + "  </property>\n"
      + "</bean>"
    );

    assertThat(context.getBean("zipkinSpanHandler", ZipkinSpanHandler.class))
      .extracting("spanReporter")
      .isEqualTo(Reporter.CONSOLE);
  }

  @Test public void errorParser() {
    context = new XmlBeans(""
      + "<bean id=\"zipkinSpanHandler\" class=\"zipkin2.reporter.beans.ZipkinSpanHandlerFactoryBean\">\n"
      + "  <property name=\"spanReporter\">\n"
      + "    <util:constant static-field=\"zipkin2.reporter.Reporter.CONSOLE\"/>\n"
      + "  </property>\n"
      + "  <property name=\"errorParser\">\n"
      + "    <util:constant static-field=\"" + getClass().getName() + ".ERROR_PARSER\"/>\n"
      + "  </property>\n"
      + "</bean>"
    );

    assertThat(context.getBean("zipkinSpanHandler", ZipkinSpanHandler.class))
      .extracting("errorParser")
      .isSameAs(ERROR_PARSER);
  }

  @Test public void alwaysReportSpans() {
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
