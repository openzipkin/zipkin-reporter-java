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
import java.util.Arrays;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import zipkin2.reporter.libthrift.LibthriftSender;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

class LibthriftSenderFactoryBeanTest {
  XmlBeans context;

  @AfterEach void close() {
    if (context != null) context.close();
  }

  @Test void host() throws MalformedURLException {
    context =
        new XmlBeans(
            ""
                + "<bean id=\"sender\" class=\"zipkin2.reporter.beans.LibthriftSenderFactoryBean\">\n"
                + "  <property name=\"host\" value=\"myhost\"/>\n"
                + "</bean>");

    assertThat(context.getBean("sender", LibthriftSender.class))
        .extracting("host")
        .isEqualTo("myhost");
  }

  @Test void connectTimeout() {
    context =
        new XmlBeans(
            ""
                + "<bean id=\"sender\" class=\"zipkin2.reporter.beans.LibthriftSenderFactoryBean\">\n"
                + "  <property name=\"host\" value=\"myhost\"/>\n"
                + "  <property name=\"connectTimeout\" value=\"0\"/>\n"
                + "</bean>");

    assertThat(context.getBean("sender", LibthriftSender.class))
        .usingRecursiveComparison()
        .isEqualTo(LibthriftSender.newBuilder().host("myhost").connectTimeout(0).build());
  }

  @Test void socketTimeout() {
    context =
        new XmlBeans(
            ""
                + "<bean id=\"sender\" class=\"zipkin2.reporter.beans.LibthriftSenderFactoryBean\">\n"
                + "  <property name=\"host\" value=\"myhost\"/>\n"
                + "  <property name=\"socketTimeout\" value=\"0\"/>\n"
                + "</bean>");

    assertThat(context.getBean("sender", LibthriftSender.class))
        .usingRecursiveComparison()
        .isEqualTo(LibthriftSender.newBuilder().host("myhost").socketTimeout(0).build());
  }

  @Test void port() {
    context =
        new XmlBeans(
            ""
                + "<bean id=\"sender\" class=\"zipkin2.reporter.beans.LibthriftSenderFactoryBean\">\n"
                + "  <property name=\"host\" value=\"myhost\"/>\n"
                + "  <property name=\"port\" value=\"1000\"/>\n"
                + "</bean>");

    assertThat(context.getBean("sender", LibthriftSender.class))
        .extracting("port")
        .isEqualTo(1000);
  }

  @Test void messageMaxBytes() {
    context =
        new XmlBeans(
            ""
                + "<bean id=\"sender\" class=\"zipkin2.reporter.beans.LibthriftSenderFactoryBean\">\n"
                + "  <property name=\"host\" value=\"myhost\"/>\n"
                + "  <property name=\"messageMaxBytes\" value=\"1024\"/>\n"
                + "</bean>");

    assertThat(context.getBean("sender", LibthriftSender.class))
        .extracting("messageMaxBytes")
        .isEqualTo(1024);
  }

  @Test void close_closesSender() {
    assertThrows(IllegalStateException.class, () -> {
      context =
          new XmlBeans(
              ""
                  + "<bean id=\"sender\" class=\"zipkin2.reporter.beans.LibthriftSenderFactoryBean\">\n"
                  + "  <property name=\"host\" value=\"myhost\"/>\n"
                  + "</bean>");

      LibthriftSender sender = context.getBean("sender", LibthriftSender.class);
      context.close();

      sender.send(Arrays.asList(new byte[0]));
    });
  }
}
