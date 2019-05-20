# zipkin-reporter-spring-beans
This module contains Spring Factory Beans that allow you to configure
tracing with only XML. Notably, this requires minimally Spring version 2.5.

## Configuration
Bean Factories exist for the following types:
* AsyncReporterFactoryBean - for configuring how often spans are sent to Zipkin
* OkHttpSenderFactoryBean - for [zipkin-sender-okhttp3](../okhttp3)
* KafkaSenderFactoryBean - for [zipkin-sender-kafka](../kafka)
* RabbitMQSenderFactoryBean - for [zipkin-sender-amqp-client](../amqp-client)
* URLConnectionSenderFactoryBean - for [zipkin-sender-urlconnection](../urlconnection)

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
