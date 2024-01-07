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

import java.util.Arrays;
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

  @Test void endpoint() {
    context = new XmlBeans(""
        + "<bean id=\"sender\" class=\"zipkin2.reporter.beans.OkHttpSenderFactoryBean\">\n"
        + "  <property name=\"endpoint\" value=\"http://localhost:9411/api/v2/spans\"/>\n"
        + "</bean>"
    );

    assertThat(context.getBean("sender", OkHttpSender.class))
        .extracting("endpoint")
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
        .extracting("client.connectTimeoutMillis")
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
        .extracting("client.writeTimeoutMillis")
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
        .extracting("client.readTimeoutMillis")
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
        .extracting("client.dispatcher.maxRequests")
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
        .extracting("compressionEnabled")
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
        .extracting("messageMaxBytes")
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
        .extracting("encoding")
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

      sender.sendSpans(Arrays.asList(new byte[]{'{', '}'}));
    });
  }
}
