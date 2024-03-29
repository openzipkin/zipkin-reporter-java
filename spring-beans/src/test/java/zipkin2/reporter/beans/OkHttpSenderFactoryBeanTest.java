/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin2.reporter.beans;

import java.util.List;
import okhttp3.HttpUrl;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import zipkin2.reporter.Encoding;
import zipkin2.reporter.okhttp3.OkHttpSender;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

class OkHttpSenderFactoryBeanTest {
  XmlBeans context;

  @AfterEach void close() {
    if (context != null) context.close();
  }

  @Test void endpointSupplierFactory() {
    context = new XmlBeans(String.format(""
      + "<bean id=\"sender\" class=\"zipkin2.reporter.beans.OkHttpSenderFactoryBean\">\n"
      + "  <property name=\"endpointSupplierFactory\">\n"
      + "    <util:constant static-field=\"%s.FACTORY\"/>\n"
      + "  </property>\n"
      + "  <property name=\"endpoint\" value=\"http://localhost:9411/api/v2/spans\"/>\n"
      + "</bean>", FakeEndpointSupplier.class.getName())
    );

    assertThat(context.getBean("sender", OkHttpSender.class))
      .extracting("delegate.endpointSupplier")
      .isEqualTo(FakeEndpointSupplier.INSTANCE);
  }

  @Test void endpoint() {
    context = new XmlBeans(""
      + "<bean id=\"sender\" class=\"zipkin2.reporter.beans.OkHttpSenderFactoryBean\">\n"
      + "  <property name=\"endpoint\" value=\"http://localhost:9411/api/v2/spans\"/>\n"
      + "</bean>"
    );

    assertThat(context.getBean("sender", OkHttpSender.class))
      .extracting("delegate.endpoint")
      .isEqualTo(HttpUrl.parse("http://localhost:9411/api/v2/spans"));
  }

  @Test void connectTimeout() {
    context = new XmlBeans(""
        + "<bean id=\"sender\" class=\"zipkin2.reporter.beans.OkHttpSenderFactoryBean\">\n"
        + "  <property name=\"endpoint\" value=\"http://localhost:9411/api/v2/spans\"/>\n"
        + "  <property name=\"connectTimeout\" value=\"1000\"/>\n"
        + "</bean>"
    );

    assertThat(context.getBean("sender", OkHttpSender.class))
        .extracting("delegate.client.connectTimeoutMillis")
        .isEqualTo(1000);
  }

  @Test void writeTimeout() {
    context = new XmlBeans(""
        + "<bean id=\"sender\" class=\"zipkin2.reporter.beans.OkHttpSenderFactoryBean\">\n"
        + "  <property name=\"endpoint\" value=\"http://localhost:9411/api/v2/spans\"/>\n"
        + "  <property name=\"writeTimeout\" value=\"1000\"/>\n"
        + "</bean>"
    );

    assertThat(context.getBean("sender", OkHttpSender.class))
        .extracting("delegate.client.writeTimeoutMillis")
        .isEqualTo(1000);
  }

  @Test void readTimeout() {
    context = new XmlBeans(""
        + "<bean id=\"sender\" class=\"zipkin2.reporter.beans.OkHttpSenderFactoryBean\">\n"
        + "  <property name=\"endpoint\" value=\"http://localhost:9411/api/v2/spans\"/>\n"
        + "  <property name=\"readTimeout\" value=\"1000\"/>\n"
        + "</bean>"
    );

    assertThat(context.getBean("sender", OkHttpSender.class))
        .extracting("delegate.client.readTimeoutMillis")
        .isEqualTo(1000);
  }

  @Test void maxRequests() {
    context = new XmlBeans(""
        + "<bean id=\"sender\" class=\"zipkin2.reporter.beans.OkHttpSenderFactoryBean\">\n"
        + "  <property name=\"endpoint\" value=\"http://localhost:9411/api/v2/spans\"/>\n"
        + "  <property name=\"maxRequests\" value=\"4\"/>\n"
        + "</bean>"
    );

    assertThat(context.getBean("sender", OkHttpSender.class))
        .extracting("delegate.client.dispatcher.maxRequests")
        .isEqualTo(4);
  }

  @Test void compressionEnabled() {
    context = new XmlBeans(""
        + "<bean id=\"sender\" class=\"zipkin2.reporter.beans.OkHttpSenderFactoryBean\">\n"
        + "  <property name=\"endpoint\" value=\"http://localhost:9411/api/v2/spans\"/>\n"
        + "  <property name=\"compressionEnabled\" value=\"false\"/>\n"
        + "</bean>"
    );

    assertThat(context.getBean("sender", OkHttpSender.class))
        .extracting("delegate.compressionEnabled")
        .isEqualTo(false);
  }

  @Test void messageMaxBytes() {
    context = new XmlBeans(""
        + "<bean id=\"sender\" class=\"zipkin2.reporter.beans.OkHttpSenderFactoryBean\">\n"
        + "  <property name=\"endpoint\" value=\"http://localhost:9411/api/v2/spans\"/>\n"
        + "  <property name=\"messageMaxBytes\" value=\"1024\"/>\n"
        + "</bean>"
    );

    assertThat(context.getBean("sender", OkHttpSender.class))
        .extracting("delegate.messageMaxBytes")
        .isEqualTo(1024);
  }

  @Test void encoding() {
    context = new XmlBeans(""
        + "<bean id=\"sender\" class=\"zipkin2.reporter.beans.OkHttpSenderFactoryBean\">\n"
        + "  <property name=\"endpoint\" value=\"http://localhost:9411/api/v2/spans\"/>\n"
        + "  <property name=\"encoding\" value=\"PROTO3\"/>\n"
        + "</bean>"
    );

    assertThat(context.getBean("sender", OkHttpSender.class))
        .extracting("delegate.encoding")
        .isEqualTo(Encoding.PROTO3);
  }

  @Test void close_closesSender() {
    assertThrows(IllegalStateException.class, () -> {
      context = new XmlBeans(""
          + "<bean id=\"sender\" class=\"zipkin2.reporter.beans.OkHttpSenderFactoryBean\">\n"
          + "  <property name=\"endpoint\" value=\"http://localhost:9411/api/v2/spans\"/>\n"
          + "</bean>"
      );

      OkHttpSender sender = context.getBean("sender", OkHttpSender.class);
      context.close();

      sender.send(List.of(new byte[]{'{', '}'}));
    });
  }
}
