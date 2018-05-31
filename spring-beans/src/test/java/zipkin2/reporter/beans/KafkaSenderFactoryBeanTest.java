/*
 * Copyright 2016-2018 The OpenZipkin Authors
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
import org.junit.After;
import org.junit.Test;
import zipkin2.codec.Encoding;
import zipkin2.reporter.kafka11.KafkaSender;

import static org.assertj.core.api.Assertions.assertThat;

public class KafkaSenderFactoryBeanTest {
  XmlBeans context;

  @After public void close() {
    if (context != null) context.close();
  }

  @Test public void bootstrapServers() {
    context = new XmlBeans(""
        + "<bean id=\"sender\" class=\"zipkin2.reporter.beans.KafkaSenderFactoryBean\">\n"
        + "  <property name=\"bootstrapServers\" value=\"localhost:9092\"/>\n"
        + "</bean>"
    );

    assertThat(context.getBean("sender", KafkaSender.class))
        .isEqualToComparingFieldByField(KafkaSender.newBuilder()
            .bootstrapServers("localhost:9092")
            .build());
  }

  @Test public void topic() {
    context = new XmlBeans(""
        + "<bean id=\"sender\" class=\"zipkin2.reporter.beans.KafkaSenderFactoryBean\">\n"
        + "  <property name=\"bootstrapServers\" value=\"localhost:9092\"/>\n"
        + "  <property name=\"topic\" value=\"zipkin2\"/>\n"
        + "</bean>"
    );

    assertThat(context.getBean("sender", KafkaSender.class))
        .isEqualToComparingFieldByField(KafkaSender.newBuilder()
            .bootstrapServers("localhost:9092")
            .topic("zipkin2").build());
  }

  @Test public void messageMaxBytes() {
    context = new XmlBeans(""
        + "<bean id=\"sender\" class=\"zipkin2.reporter.beans.KafkaSenderFactoryBean\">\n"
        + "  <property name=\"bootstrapServers\" value=\"localhost:9092\"/>\n"
        + "  <property name=\"messageMaxBytes\" value=\"1024\"/>\n"
        + "</bean>"
    );

    assertThat(context.getBean("sender", KafkaSender.class))
        .extracting("messageMaxBytes")
        .containsExactly(1024);
  }

  @Test public void encoding() {
    context = new XmlBeans(""
        + "<bean id=\"sender\" class=\"zipkin2.reporter.beans.KafkaSenderFactoryBean\">\n"
        + "  <property name=\"bootstrapServers\" value=\"localhost:9092\"/>\n"
        + "  <property name=\"encoding\" value=\"PROTO3\"/>\n"
        + "</bean>"
    );

    assertThat(context.getBean("sender", KafkaSender.class))
        .extracting("encoding")
        .containsExactly(Encoding.PROTO3);
  }

  @Test(expected = IllegalStateException.class) public void close_closesSender() {
    context = new XmlBeans(""
        + "<bean id=\"sender\" class=\"zipkin2.reporter.beans.KafkaSenderFactoryBean\">\n"
        + "  <property name=\"bootstrapServers\" value=\"localhost:9092\"/>\n"
        + "</bean>"
    );

    KafkaSender sender = context.getBean("sender", KafkaSender.class);
    context.close();

    sender.sendSpans(Arrays.asList(new byte[] {'{', '}'}));
  }
}
