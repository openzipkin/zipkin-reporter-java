/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin2.reporter.beans;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import zipkin2.reporter.Encoding;
import zipkin2.reporter.pulsar.PulsarSender;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

class PulsarSenderFactoryBeanTest {
  XmlBeans context;

  @AfterEach void close() {
    if (context != null) context.close();
  }

  @Test void serviceUrlAndTopic() {
    context = new XmlBeans(""
        + "<bean id=\"sender\" class=\"zipkin2.reporter.beans.PulsarSenderFactoryBean\">\n"
        + "  <property name=\"serviceUrl\" value=\"pulsar://localhost:6650\"/>\n"
        + "  <property name=\"topic\" value=\"zhouzixin\"/>\n"
        + "</bean>"
    );

    assertThat(context.getBean("sender", PulsarSender.class))
        .usingRecursiveComparison()
        .isEqualTo(PulsarSender.newBuilder()
            .serviceUrl("pulsar://localhost:6650")
            .topic("zhouzixin").build());
  }

  @Test void messageMaxBytes() {
    context = new XmlBeans(""
        + "<bean id=\"sender\" class=\"zipkin2.reporter.beans.PulsarSenderFactoryBean\">\n"
        + "  <property name=\"serviceUrl\" value=\"pulsar://localhost:6650\"/>\n"
        + "  <property name=\"topic\" value=\"zhouzixin\"/>\n"
        + "  <property name=\"messageMaxBytes\" value=\"1024\"/>\n"
        + "</bean>"
    );

    assertThat(context.getBean("sender", PulsarSender.class))
        .extracting("messageMaxBytes")
        .isEqualTo(1024);
  }

  @Test void encoding() {
    context = new XmlBeans(""
        + "<bean id=\"sender\" class=\"zipkin2.reporter.beans.PulsarSenderFactoryBean\">\n"
        + "  <property name=\"serviceUrl\" value=\"pulsar://localhost:6650\"/>\n"
        + "  <property name=\"topic\" value=\"zhouzixin\"/>\n"
        + "  <property name=\"encoding\" value=\"PROTO3\"/>\n"
        + "</bean>"
    );

    assertThat(context.getBean("sender", PulsarSender.class))
        .extracting("encoding")
        .isEqualTo(Encoding.PROTO3);
  }

  @Test void close_closesSender() {
    assertThrows(IllegalStateException.class, () -> {
      context = new XmlBeans(""
          + "<bean id=\"sender\" class=\"zipkin2.reporter.beans.PulsarSenderFactoryBean\">\n"
          + "  <property name=\"serviceUrl\" value=\"pulsar://localhost:6650\"/>\n"
          + "  <property name=\"topic\" value=\"zhouzixin\"/>\n"
          + "</bean>"
      );

      PulsarSender sender = context.getBean("sender", PulsarSender.class);
      context.close();

      sender.send(Arrays.asList(new byte[]{'{', '}'}));
    });
  }
}
