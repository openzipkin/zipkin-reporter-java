/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin2.reporter.beans;

import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.beans.factory.xml.XmlBeanDefinitionReader;
import org.springframework.core.io.ByteArrayResource;

import static java.nio.charset.StandardCharsets.UTF_8;

class XmlBeans {
  final DefaultListableBeanFactory beanFactory = new DefaultListableBeanFactory();

  XmlBeans(String... beans) {
    StringBuilder joined = new StringBuilder();
    for (String bean : beans) {
      joined.append(bean).append('\n');
    }
    new XmlBeanDefinitionReader(beanFactory).loadBeanDefinitions(
        new ByteArrayResource(beans(joined.toString()).getBytes(UTF_8))
    );
  }

  static String beans(String bean) {
    return "<beans xmlns=\"http://www.springframework.org/schema/beans\"\n"
        + "    xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"\n"
        + "    xmlns:util=\"http://www.springframework.org/schema/util\"\n"
        + "    xsi:schemaLocation=\"\n"
        + "        http://www.springframework.org/schema/beans\n"
        + "        http://www.springframework.org/schema/beans/spring-beans-2.5.xsd\n"
        + "        http://www.springframework.org/schema/util\n"
        + "        http://www.springframework.org/schema/util/spring-util-2.5.xsd\">\n"
        + bean
        + "</beans>";
  }

  <T> T getBean(String name, Class<T> requiredType) {
    return (T) beanFactory.getBean(name, requiredType);
  }

  void close() {
    beanFactory.destroySingletons();
  }
}
