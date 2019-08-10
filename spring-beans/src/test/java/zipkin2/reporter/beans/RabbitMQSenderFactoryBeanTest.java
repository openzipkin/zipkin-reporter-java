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

import com.rabbitmq.client.Address;
import java.util.Arrays;
import org.junit.After;
import org.junit.Test;
import zipkin2.codec.Encoding;
import zipkin2.reporter.amqp.RabbitMQSender;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;

public class RabbitMQSenderFactoryBeanTest {
  XmlBeans context;

  @After public void close() {
    if (context != null) context.close();
  }

  @Test public void addresses() {
    context = new XmlBeans(""
        + "<bean id=\"sender\" class=\"zipkin2.reporter.beans.RabbitMQSenderFactoryBean\">\n"
        + "  <property name=\"addresses\" value=\"localhost\"/>\n"
        + "</bean>"
    );

    assertThat(context.getBean("sender", RabbitMQSender.class))
        .extracting("addresses")
        .isEqualTo(asList(new Address("localhost")));
  }

  @Test public void queue() {
    context = new XmlBeans(""
        + "<bean id=\"sender\" class=\"zipkin2.reporter.beans.RabbitMQSenderFactoryBean\">\n"
        + "  <property name=\"addresses\" value=\"localhost\"/>\n"
        + "  <property name=\"queue\" value=\"zipkin2\"/>\n"
        + "</bean>"
    );

    assertThat(context.getBean("sender", RabbitMQSender.class))
        .extracting("queue")
        .isEqualTo("zipkin2");
  }

  @Test public void connectionTimeout() {
    context = new XmlBeans(""
        + "<bean id=\"sender\" class=\"zipkin2.reporter.beans.RabbitMQSenderFactoryBean\">\n"
        + "  <property name=\"addresses\" value=\"localhost\"/>\n"
        + "  <property name=\"connectionTimeout\" value=\"0\"/>\n"
        + "</bean>"
    );

    assertThat(context.getBean("sender", RabbitMQSender.class))
        .extracting("connectionFactory.connectionTimeout")
        .isEqualTo(0);
  }

  @Test public void virtualHost() {
    context = new XmlBeans(""
        + "<bean id=\"sender\" class=\"zipkin2.reporter.beans.RabbitMQSenderFactoryBean\">\n"
        + "  <property name=\"addresses\" value=\"localhost\"/>\n"
        + "  <property name=\"virtualHost\" value=\"zipkin3\"/>\n"
        + "</bean>"
    );

    assertThat(context.getBean("sender", RabbitMQSender.class))
        .extracting("connectionFactory.virtualHost")
        .isEqualTo("zipkin3");
  }

  @Test public void usernamePassword() {
    context = new XmlBeans(""
        + "<bean id=\"sender\" class=\"zipkin2.reporter.beans.RabbitMQSenderFactoryBean\">\n"
        + "  <property name=\"addresses\" value=\"localhost\"/>\n"
        + "  <property name=\"username\" value=\"foo\"/>\n"
        + "  <property name=\"password\" value=\"bar\"/>\n"
        + "</bean>"
    );

    assertThat(context.getBean("sender", RabbitMQSender.class))
        .extracting("connectionFactory.username", "connectionFactory.password")
        .isEqualTo(asList("foo", "bar"));
  }

  @Test public void messageMaxBytes() {
    context = new XmlBeans(""
        + "<bean id=\"sender\" class=\"zipkin2.reporter.beans.RabbitMQSenderFactoryBean\">\n"
        + "  <property name=\"addresses\" value=\"localhost\"/>\n"
        + "  <property name=\"messageMaxBytes\" value=\"1024\"/>\n"
        + "</bean>"
    );

    assertThat(context.getBean("sender", RabbitMQSender.class))
        .extracting("messageMaxBytes")
        .isEqualTo(1024);
  }

  @Test public void encoding() {
    context = new XmlBeans(""
        + "<bean id=\"sender\" class=\"zipkin2.reporter.beans.RabbitMQSenderFactoryBean\">\n"
        + "  <property name=\"addresses\" value=\"localhost\"/>\n"
        + "  <property name=\"encoding\" value=\"PROTO3\"/>\n"
        + "</bean>"
    );

    assertThat(context.getBean("sender", RabbitMQSender.class))
        .extracting("encoding")
        .isEqualTo(Encoding.PROTO3);
  }

  @Test(expected = IllegalStateException.class) public void close_closesSender() {
    context = new XmlBeans(""
        + "<bean id=\"sender\" class=\"zipkin2.reporter.beans.RabbitMQSenderFactoryBean\">\n"
        + "  <property name=\"addresses\" value=\"localhost\"/>\n"
        + "</bean>"
    );

    RabbitMQSender sender = context.getBean("sender", RabbitMQSender.class);
    context.close();

    sender.sendSpans(asList(new byte[] {'{', '}'}));
  }
}
