/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin2.reporter.beans;

import java.util.Arrays;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import zipkin2.reporter.Encoding;
import zipkin2.reporter.kafka.KafkaSender;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

class KafkaSenderFactoryBeanTest {
  XmlBeans context;

  @AfterEach void close() {
    if (context != null) context.close();
  }

  @Test void bootstrapServers() {
    context = new XmlBeans(""
      + "<bean id=\"sender\" class=\"zipkin2.reporter.beans.KafkaSenderFactoryBean\">\n"
      + "  <property name=\"bootstrapServers\" value=\"localhost:9092\"/>\n"
      + "</bean>"
    );

    assertThat(context.getBean("sender", KafkaSender.class))
      .usingRecursiveComparison()
      .isEqualTo(KafkaSender.newBuilder()
        .bootstrapServers("localhost:9092")
        .build());
  }

  @Test void topic() {
    context = new XmlBeans(""
      + "<bean id=\"sender\" class=\"zipkin2.reporter.beans.KafkaSenderFactoryBean\">\n"
      + "  <property name=\"bootstrapServers\" value=\"localhost:9092\"/>\n"
      + "  <property name=\"topic\" value=\"zipkin2\"/>\n"
      + "</bean>"
    );

    assertThat(context.getBean("sender", KafkaSender.class))
      .usingRecursiveComparison()
      .isEqualTo(KafkaSender.newBuilder()
        .bootstrapServers("localhost:9092")
        .topic("zipkin2").build());
  }

  @Test void messageMaxBytes() {
    context = new XmlBeans(""
      + "<bean id=\"sender\" class=\"zipkin2.reporter.beans.KafkaSenderFactoryBean\">\n"
      + "  <property name=\"bootstrapServers\" value=\"localhost:9092\"/>\n"
      + "  <property name=\"messageMaxBytes\" value=\"1024\"/>\n"
      + "</bean>"
    );

    assertThat(context.getBean("sender", KafkaSender.class))
      .extracting("messageMaxBytes")
      .isEqualTo(1024);
  }

  @Test void encoding() {
    context = new XmlBeans(""
      + "<bean id=\"sender\" class=\"zipkin2.reporter.beans.KafkaSenderFactoryBean\">\n"
      + "  <property name=\"bootstrapServers\" value=\"localhost:9092\"/>\n"
      + "  <property name=\"encoding\" value=\"PROTO3\"/>\n"
      + "</bean>"
    );

    assertThat(context.getBean("sender", KafkaSender.class))
      .extracting("encoding")
      .isEqualTo(Encoding.PROTO3);
  }

  @Test void close_closesSender() {
    assertThrows(IllegalStateException.class, () -> {
      context = new XmlBeans(""
        + "<bean id=\"sender\" class=\"zipkin2.reporter.beans.KafkaSenderFactoryBean\">\n"
        + "  <property name=\"bootstrapServers\" value=\"localhost:9092\"/>\n"
        + "</bean>"
      );

      KafkaSender sender = context.getBean("sender", KafkaSender.class);
      context.close();

      sender.send(Arrays.asList(new byte[] {'{', '}'}));
    });
  }
}
