# zipkin-reporter-spring-beans
This module contains Spring Factory Beans that allow you to configure
zipkinSpanHandler with only XML. Notably, this requires minimally Spring version 2.5.

## Configuration
Bean Factories exist for the following types:
* AsyncReporterFactoryBean - for configuring how often spans are sent to Zipkin
* ActiveMQSenderFactoryBean - for [zipkin-sender-activemq-client](../activemq-client)
* OkHttpSenderFactoryBean - for [zipkin-sender-okhttp3](../okhttp3)
* KafkaSenderFactoryBean - for [zipkin-sender-kafka](../kafka)
* RabbitMQSenderFactoryBean - for [zipkin-sender-amqp-client](../amqp-client)
* URLConnectionSenderFactoryBean - for [zipkin-sender-urlconnection](../urlconnection)
* ZipkinSpanHandlerFactoryBeanTest - for [brave](https://github.com/openzipkin/brave)

Here's a basic example
```xml
  <bean id="spanReporter" class="zipkin2.reporter.beans.AsyncReporterFactoryBean">
    <property name="sender">
      <bean class="zipkin2.reporter.beans.OkHttpSenderFactoryBean">
        <property name="endpoint" value="http://localhost:9411/api/v2/spans"/>
      </bean>
    </property>
  </bean>
```

Here's an example with Kafka configuration and extended configuration:
```xml
  <bean id="sender" class="zipkin2.reporter.beans.KafkaSenderFactoryBean">
    <property name="bootstrapServers" value="192.168.99.100:9092"/>
    <!-- if using a zipkin server 2.8+, you can send in binary format -->
    <property name="encoding" value="PROTO3"/>
    <property name="topic" value="test_zipkin"/>
  </bean>

  <bean id="spanReporter" class="zipkin2.reporter.beans.AsyncReporterFactoryBean">
    <property name="sender" ref="sender"/>
    <!-- wait up to half a second for any in-flight spans on close -->
    <property name="closeTimeout" value="500"/>
  </bean>
```

Here's an example integrating with [Brave](https://github.com/openzipkin/brave/tree/master/spring-beans)

```xml
<bean id="tracing" class="brave.spring.beans.TracingFactoryBean">
  <property name="localServiceName" value="${zipkin.service}"/>
  <property name="finishedSpanHandlers">
    <bean class="zipkin2.reporter.beans.ZipkinSpanHandlerFactoryBean">
      <property name="spanReporter" ref="spanReporter"/>
    </bean>
  </property>
</bean>
```

*Note*: The minimum version of Brave is 5.6. If you are using a version of Brave before 5.12, you
will also need to set the "spanReporter" field to `Reporter.NOOP`. Otherwise, you will see a log
message for each span.

Ex.
```xml
<bean id="tracing" class="brave.spring.beans.TracingFactoryBean">
  <property name="localServiceName" value="${zipkin.service}"/>
  <property name="finishedSpanHandlers">
    <bean class="zipkin2.reporter.beans.ZipkinSpanHandlerFactoryBean">
      <property name="spanReporter" ref="spanReporter"/>
    </bean>
  </property>
  <!-- Suppress the logging reporter -->
  <property name="spanReporter">
    <util:constant static-field="zipkin2.reporter.Reporter.NOOP"/>
  </property>
</bean>
```
