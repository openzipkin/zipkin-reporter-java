/*
 * Copyright 2016-2023 The OpenZipkin Authors
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
package zipkin2.reporter.libthrift;

import java.io.Closeable;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.thrift.TConfiguration;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.transport.layered.TFramedTransport;

final class ScribeClient implements Closeable {
  static final Logger logger = Logger.getLogger(ScribeClient.class.getName());
  static final byte[] category = new byte[] {'z', 'i', 'p', 'k', 'i', 'n'};

  final String host;
  final int port;
  final int socketTimeout;
  final int connectTimeout;

  volatile TSocket socket;
  volatile TBinaryProtocol prot;

  /**
   * This defers opening a socket until the first call to {@link #log}, to
   * accommodate a server that's down when the client initializes.
   */
  ScribeClient(String host, int port, int socketTimeout, int connectTimeout) {
    this.host = host;
    this.port = port;
    this.socketTimeout = socketTimeout;
    this.connectTimeout = connectTimeout;
  }

  static int messageSizeInBytes(int spanSizeInBytes) {
    return InternalScribeCodec.messageSizeInBytes(category, spanSizeInBytes);
  }

  static int messageSizeInBytes(List<byte[]> encodedSpans) {
    return InternalScribeCodec.messageSizeInBytes(category, encodedSpans);
  }

  private int seqid_;

  boolean log(List<byte[]> encodedSpans) throws TException {
    try {
      // Starting in version 0.14, TSocket opens a socket inside its
      // constructor, which we defer so that the server can be initially down.
      if (socket == null) {
        synchronized (this) {
          if (socket == null) {
            socket = new TSocket(new TConfiguration(), host, port, socketTimeout, connectTimeout);
            prot = new TBinaryProtocol(new TFramedTransport(socket));
          }
        }
      }

      if (!socket.isOpen()) socket.open();
      InternalScribeCodec.writeLogRequest(category, encodedSpans, ++seqid_, prot);
      prot.getTransport().flush();
      return InternalScribeCodec.readLogResponse(seqid_, prot);
    } catch (TTransportException e) {
      logger.log(Level.FINE, "Transport exception. recreating socket", e);
      close();
      seqid_ = 0;
      throw e;
    }
  }

  @Override public void close() {
    TSocket maybe = this.socket;
    if (maybe != null) maybe.close();
  }
}
