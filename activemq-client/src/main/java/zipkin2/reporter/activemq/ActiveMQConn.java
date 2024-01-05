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
