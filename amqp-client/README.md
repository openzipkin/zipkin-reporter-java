# zipkin-sender-amqp-client
This module contains a span sender for [RabbitMQ](https://github.com/rabbitmq/rabbitmq-java-client).

Please view [RabbitMQSender](src/main/java/zipkin2/reporter/amqp/RabbitMQSender.java)
for usage details.

## Compatability

In order to keep new users current, this assigns the most recent major version
of amqp-client 5.x. This reduces distraction around CVEs. That said, this
module is tested to be runtime compatible with 4.x, for users who need to
support Java 6 or 7 use cases.
