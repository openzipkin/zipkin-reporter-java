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

import java.util.Arrays;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import zipkin2.codec.Encoding;
import zipkin2.reporter.activemq.ActiveMQSender;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ActiveMQSenderFactoryBeanTest {
  XmlBeans context;

  @AfterEach void close() {
    if (context != null) context.close();
  }

  @Test void url() {
    String brokerUrl = "ssl://abcd.mq.ap-southeast-1.amazonaws.com:61617";
    context = new XmlBeans(""
      + "<bean id=\"sender\" class=\"zipkin2.reporter.beans.ActiveMQSenderFactoryBean\">\n"
      + "  <property name=\"url\" value=\"" + brokerUrl + "\"/>\n"
      + "</bean>"
    );

    assertThat(context.getBean("sender", ActiveMQSender.class))
      .extracting("lazyInit.connectionFactory.brokerURL")
      .isEqualTo(brokerUrl);
  }

  @Test void connectionIdPrefix() {
    String connectionIdPrefix = "zipkin-reporter2";
    context = new XmlBeans(""
      + "<bean id=\"sender\" class=\"zipkin2.reporter.beans.ActiveMQSenderFactoryBean\">\n"
      + "  <property name=\"url\" value=\"tcp://localhost:61616\"/>\n"
      + "  <property name=\"connectionIdPrefix\" value=\"" + connectionIdPrefix + "\"/>\n"
      + "</bean>"
    );

    assertThat(context.getBean("sender", ActiveMQSender.class))
      .extracting("lazyInit.connectionFactory.connectionIDPrefix")
      .isEqualTo(connectionIdPrefix);
  }

  @Test void clientIdPrefix() {
    String clientIdPrefix = "zipkin-reporter2";
    context = new XmlBeans(""
      + "<bean id=\"sender\" class=\"zipkin2.reporter.beans.ActiveMQSenderFactoryBean\">\n"
      + "  <property name=\"url\" value=\"tcp://localhost:61616\"/>\n"
      + "  <property name=\"clientIdPrefix\" value=\"" + clientIdPrefix + "\"/>\n"
      + "</bean>"
    );

    assertThat(context.getBean("sender", ActiveMQSender.class))
      .extracting("lazyInit.connectionFactory.clientIDPrefix")
      .isEqualTo(clientIdPrefix);
  }

  @Test void queue() {
    context = new XmlBeans(""
      + "<bean id=\"sender\" class=\"zipkin2.reporter.beans.ActiveMQSenderFactoryBean\">\n"
      + "  <property name=\"url\" value=\"tcp://localhost:61616\"/>\n"
      + "  <property name=\"queue\" value=\"zipkin2\"/>\n"
      + "</bean>"
    );

    assertThat(context.getBean("sender", ActiveMQSender.class))
      .extracting("lazyInit.queue")
      .isEqualTo("zipkin2");
  }

  @Test void usernamePassword() {
    context = new XmlBeans(""
      + "<bean id=\"sender\" class=\"zipkin2.reporter.beans.ActiveMQSenderFactoryBean\">\n"
      + "  <property name=\"url\" value=\"tcp://localhost:61616\"/>\n"
      + "  <property name=\"username\" value=\"foo\"/>\n"
      + "  <property name=\"password\" value=\"bar\"/>\n"
      + "</bean>"
    );

    assertThat(context.getBean("sender", ActiveMQSender.class))
      .extracting("lazyInit.connectionFactory.userName", "lazyInit.connectionFactory.password")
      .containsExactly("foo", "bar");
  }

  @Test void messageMaxBytes() {
    context = new XmlBeans(""
      + "<bean id=\"sender\" class=\"zipkin2.reporter.beans.ActiveMQSenderFactoryBean\">\n"
      + "  <property name=\"url\" value=\"tcp://localhost:61616\"/>\n"
      + "  <property name=\"messageMaxBytes\" value=\"1024\"/>\n"
      + "</bean>"
    );

    assertThat(context.getBean("sender", ActiveMQSender.class))
      .extracting("messageMaxBytes")
      .isEqualTo(1024);
  }

  @Test void encoding() {
    context = new XmlBeans(""
      + "<bean id=\"sender\" class=\"zipkin2.reporter.beans.ActiveMQSenderFactoryBean\">\n"
      + "  <property name=\"url\" value=\"tcp://localhost:61616\"/>\n"
      + "  <property name=\"encoding\" value=\"PROTO3\"/>\n"
      + "</bean>"
    );

    assertThat(context.getBean("sender", ActiveMQSender.class))
      .extracting("encoding")
      .isEqualTo(Encoding.PROTO3);
  }

  @Test void close_closesSender() {
    assertThrows(IllegalStateException.class, () -> {
      context = new XmlBeans(""
          + "<bean id=\"sender\" class=\"zipkin2.reporter.beans.ActiveMQSenderFactoryBean\">\n"
          + "  <property name=\"url\" value=\"tcp://localhost:61616\"/>\n"
          + "</bean>"
      );

      ActiveMQSender sender = context.getBean("sender", ActiveMQSender.class);
      context.close();

      sender.sendSpans(Arrays.asList(new byte[]{'{', '}'}));
    });
  }
}
