/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin2.reporter.beans;

import java.net.MalformedURLException;
import java.net.URI;
import java.util.List;
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

  @Test void endpointSupplierFactory() {
    context = new XmlBeans(String.format(""
      + "<bean id=\"sender\" class=\"zipkin2.reporter.beans.URLConnectionSenderFactoryBean\">\n"
      + "  <property name=\"endpointSupplierFactory\">\n"
      + "    <util:constant static-field=\"%s.FACTORY\"/>\n"
      + "  </property>\n"
      + "  <property name=\"endpoint\" value=\"http://localhost:9411/api/v2/spans\"/>\n"
      + "</bean>", FakeEndpointSupplier.class.getName())
    );

    assertThat(context.getBean("sender", URLConnectionSender.class))
      .extracting("delegate.endpointSupplier")
      .isEqualTo(FakeEndpointSupplier.INSTANCE);
  }

  @Test void endpoint() throws MalformedURLException {
    context = new XmlBeans(""
      + "<bean id=\"sender\" class=\"zipkin2.reporter.beans.URLConnectionSenderFactoryBean\">\n"
      + "  <property name=\"endpoint\" value=\"http://localhost:9411/api/v2/spans\"/>\n"
      + "</bean>"
    );

    assertThat(context.getBean("sender", URLConnectionSender.class))
      .extracting("delegate.endpoint")
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
        .extracting("delegate.compressionEnabled")
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
        .extracting("delegate.messageMaxBytes")
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
        .extracting("delegate.encoding")
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

      sender.send(List.of(new byte[]{'{', '}'}));
    });
  }
}
