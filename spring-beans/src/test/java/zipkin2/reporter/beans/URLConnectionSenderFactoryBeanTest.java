/*
 * Copyright 2016-2024 The OpenZipkin Authors
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

import java.net.MalformedURLException;
import java.net.URI;
import java.util.Arrays;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import zipkin2.reporter.Encoding;
import zipkin2.reporter.urlconnection.URLConnectionSender;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

class URLConnectionSenderFactoryBeanTest {
  XmlBeans context;

  @AfterEach void close() {
    if (context != null) context.close();
  }

  @Test void endpoint() throws MalformedURLException {
    context = new XmlBeans(""
        + "<bean id=\"sender\" class=\"zipkin2.reporter.beans.URLConnectionSenderFactoryBean\">\n"
        + "  <property name=\"endpoint\" value=\"http://localhost:9411/api/v2/spans\"/>\n"
        + "</bean>"
    );

    assertThat(context.getBean("sender", URLConnectionSender.class))
        .extracting("endpoint")
        .isEqualTo(URI.create("http://localhost:9411/api/v2/spans").toURL());
  }

  @Test void connectTimeout() {
    context = new XmlBeans(""
        + "<bean id=\"sender\" class=\"zipkin2.reporter.beans.URLConnectionSenderFactoryBean\">\n"
        + "  <property name=\"endpoint\" value=\"http://localhost:9411/api/v2/spans\"/>\n"
        + "  <property name=\"connectTimeout\" value=\"0\"/>\n"
        + "</bean>"
    );

    assertThat(context.getBean("sender", URLConnectionSender.class))
        .usingRecursiveComparison()
        .isEqualTo((URLConnectionSender.newBuilder()
            .endpoint("http://localhost:9411/api/v2/spans")
            .connectTimeout(0)
            .build()));
  }

  @Test void readTimeout() {
    context = new XmlBeans(""
        + "<bean id=\"sender\" class=\"zipkin2.reporter.beans.URLConnectionSenderFactoryBean\">\n"
        + "  <property name=\"endpoint\" value=\"http://localhost:9411/api/v2/spans\"/>\n"
        + "  <property name=\"readTimeout\" value=\"0\"/>\n"
        + "</bean>"
    );

    assertThat(context.getBean("sender", URLConnectionSender.class))
        .usingRecursiveComparison()
        .isEqualTo((URLConnectionSender.newBuilder()
            .endpoint("http://localhost:9411/api/v2/spans")
            .readTimeout(0).build()));
  }

  @Test void compressionEnabled() {
    context = new XmlBeans(""
        + "<bean id=\"sender\" class=\"zipkin2.reporter.beans.URLConnectionSenderFactoryBean\">\n"
        + "  <property name=\"endpoint\" value=\"http://localhost:9411/api/v2/spans\"/>\n"
        + "  <property name=\"compressionEnabled\" value=\"false\"/>\n"
        + "</bean>"
    );

    assertThat(context.getBean("sender", URLConnectionSender.class))
        .extracting("compressionEnabled")
        .isEqualTo(false);
  }

  @Test void messageMaxBytes() {
    context = new XmlBeans(""
        + "<bean id=\"sender\" class=\"zipkin2.reporter.beans.URLConnectionSenderFactoryBean\">\n"
        + "  <property name=\"endpoint\" value=\"http://localhost:9411/api/v2/spans\"/>\n"
        + "  <property name=\"messageMaxBytes\" value=\"1024\"/>\n"
        + "</bean>"
    );

    assertThat(context.getBean("sender", URLConnectionSender.class))
        .extracting("messageMaxBytes")
        .isEqualTo(1024);
  }

  @Test void encoding() {
    context = new XmlBeans(""
        + "<bean id=\"sender\" class=\"zipkin2.reporter.beans.URLConnectionSenderFactoryBean\">\n"
        + "  <property name=\"endpoint\" value=\"http://localhost:9411/api/v2/spans\"/>\n"
        + "  <property name=\"encoding\" value=\"PROTO3\"/>\n"
        + "</bean>"
    );

    assertThat(context.getBean("sender", URLConnectionSender.class))
        .extracting("encoding")
        .isEqualTo(Encoding.PROTO3);
  }

  @Test void close_closesSender() {
    assertThrows(IllegalStateException.class, () -> {
      context = new XmlBeans(""
          + "<bean id=\"sender\" class=\"zipkin2.reporter.beans.URLConnectionSenderFactoryBean\">\n"
          + "  <property name=\"endpoint\" value=\"http://localhost:9411/api/v2/spans\"/>\n"
          + "</bean>"
      );

      URLConnectionSender sender = context.getBean("sender", URLConnectionSender.class);
      context.close();

      sender.send(Arrays.asList(new byte[]{'{', '}'}));
    });
  }
}
