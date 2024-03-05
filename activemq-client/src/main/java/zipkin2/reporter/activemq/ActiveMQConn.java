/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin2.reporter.activemq;

import java.io.Closeable;
import java.io.IOException;
import javax.jms.JMSException;
import javax.jms.QueueSender;
import javax.jms.QueueSession;
import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.transport.TransportListener;
import zipkin2.reporter.CheckResult;

final class ActiveMQConn implements TransportListener, Closeable {
  static final CheckResult
    CLOSED = CheckResult.failed(new IllegalStateException("Collector intentionally closed")),
    INTERRUPTION = CheckResult.failed(new IOException("Recoverable error on ActiveMQ connection"));

  final ActiveMQConnection connection;
  final QueueSession session;
  final QueueSender sender;

  volatile CheckResult checkResult = CheckResult.OK;

  ActiveMQConn(ActiveMQConnection connection, QueueSession session, QueueSender sender) {
    this.connection = connection;
    this.session = session;
    this.sender = sender;
    connection.addTransportListener(this);
  }

  @Override public void onCommand(Object o) {
  }

  @Override public void onException(IOException error) {
    checkResult = CheckResult.failed(error);
  }

  @Override public void transportInterupted() {
    checkResult = INTERRUPTION;
  }

  @Override public void transportResumed() {
    checkResult = CheckResult.OK;
  }

  @Override public void close() {
    if (checkResult == CLOSED) return;
    checkResult = CLOSED;
    connection.removeTransportListener(this);
    try {
      sender.close();
      session.close();
      connection.close();
    } catch (JMSException ignored) {
    }
  }
}
