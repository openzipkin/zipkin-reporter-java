/**
 * Copyright 2016-2017 The OpenZipkin Authors
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
package zipkin.reporter.libthrift;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransportException;

final class ScribeClient implements Closeable {
  static final Logger logger = Logger.getLogger(ScribeClient.class.getName());
  static final byte[] category = new byte[] {'z', 'i', 'p', 'k', 'i', 'n'};

  final TSocket socket;
  final TBinaryProtocol prot;

  ScribeClient(String host, int port, int socketTimeout, int connectTimeout) {
    socket = new TSocket(host, port, socketTimeout, connectTimeout);
    prot = new TBinaryProtocol(new TFramedTransport(socket));
  }

  static int messageSizeInBytes(List<byte[]> encodedSpans) {
    return InternalScribeCodec.messageSizeInBytes(category, encodedSpans);
  }

  private int seqid_;

  boolean log(List<byte[]> encodedSpans) throws TException {
    try {
      if (!socket.isOpen()) socket.open();
      InternalScribeCodec.writeLogRequest(category, encodedSpans, ++seqid_, prot);
      prot.getTransport().flush();
      return InternalScribeCodec.readLogResponse(seqid_, prot);
    } catch (TTransportException e) {
      logger.log(Level.FINE, "Transport exception. recreating socket", e);
      socket.close();
      seqid_ = 0;
      throw e;
    }
  }

  @Override public void close() throws IOException {
    socket.close();
  }
}
