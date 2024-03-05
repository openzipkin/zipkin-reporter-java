/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin2.reporter.activemq;

import java.io.IOException;
import javax.jms.JMSException;
import javax.jms.Queue;
import javax.jms.QueueSender;
import javax.jms.QueueSession;
import javax.jms.Session;
import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;

import static zipkin2.reporter.activemq.ActiveMQSender.ioException;

/**
 * Lazy creates a connection and registers a message listener up to the specified concurrency level.
 * This listener will also receive health notifications.
 */
final class LazyInit {
  final ActiveMQConnectionFactory connectionFactory;
  final String queue;

  volatile ActiveMQConn result;

  LazyInit(ActiveMQSender.Builder builder) {
    connectionFactory = builder.connectionFactory;
    queue = builder.queue;
  }

  ActiveMQConn get() throws IOException {
    if (result == null) {
      synchronized (this) {
        if (result == null) result = doGet();
      }
    }
    return result;
  }

  void close() {
    ActiveMQConn maybe = result;
    if (maybe != null) result.close();
  }

  ActiveMQConn doGet() throws IOException {
    final ActiveMQConnection connection;
    try {
      connection = (ActiveMQConnection) connectionFactory.createQueueConnection();
      connection.start();
    } catch (JMSException e) {
      throw ioException("Unable to establish connection to ActiveMQ broker: ", e);
    }

    try {
      // Pass redundant info as we can't use default method in activeMQ
      QueueSession session = connection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
      // No need to do anything on ActiveMQ side as physical queues are created on demand
      Queue destination = session.createQueue(queue);
      QueueSender sender = session.createSender(destination);
      return new ActiveMQConn(connection, session, sender);
    } catch (JMSException e) {
      try {
        connection.close();
      } catch (JMSException ignored) {
      }
      throw ioException("Unable to create queueSender(" + queue + "): ", e);
    }
  }
}
