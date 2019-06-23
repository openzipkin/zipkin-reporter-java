/*
 * Copyright 2016-2019 The OpenZipkin Authors
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

  void close() throws IOException {
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
