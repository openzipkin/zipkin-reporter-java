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
