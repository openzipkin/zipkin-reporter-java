/**
 * Copyright 2016 The OpenZipkin Authors
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
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TField;
import org.apache.thrift.protocol.TList;
import org.apache.thrift.protocol.TMessage;
import org.apache.thrift.protocol.TMessageType;
import org.apache.thrift.protocol.TProtocolUtil;
import org.apache.thrift.protocol.TType;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransportException;

import static org.apache.thrift.TApplicationException.BAD_SEQUENCE_ID;
import static org.apache.thrift.TApplicationException.MISSING_RESULT;

final class ScribeClient implements Closeable {
  static final Logger logger = Logger.getLogger(ScribeClient.class.getName());
  static final byte[] category = "zipkin".getBytes();

  final TSocket socket;
  final TBinaryProtocol prot;

  ScribeClient(String host, int port, int socketTimeout, int connectTimeout) {
    socket = new TSocket(host, port, socketTimeout, connectTimeout);
    prot = new TBinaryProtocol(new TFramedTransport(socket));
  }

  private int seqid_;

  ResultCode log(List<byte[]> encodedSpans) throws TException {
    try {
      if (!socket.isOpen()) socket.open();
      return doLog(encodedSpans);
    } catch (TTransportException e) {
      logger.log(Level.FINE, "Transport exception. recreating socket", e);
      socket.close();
      seqid_ = 0;
      throw e;
    }
  }

  private ResultCode doLog(List<byte[]> encodedSpans) throws TException {
    sendMessage(encodedSpans);
    prot.getTransport().flush();

    TMessage msg = prot.readMessageBegin();
    if (msg.type == TMessageType.EXCEPTION) {
      throw TApplicationException.read(prot);
    } else if (msg.seqid != seqid_) {
      throw new TApplicationException(BAD_SEQUENCE_ID, "Log failed: out of sequence response");
    }

    ResultCode result = parseResponse();
    if (result != null) return result;
    throw new TApplicationException(MISSING_RESULT, "Log failed: unknown result");
  }

  static final TField MESSAGES_FIELD_DESC = new TField("messages", TType.LIST, (short) 1);

  static int messageSizeInBytes(List<byte[]> encodedSpans) {
    int sizeInBytes = 12 + 3; // messageBegin = overhead + size of "Log"
    sizeInBytes += 5; // FieldBegin
    sizeInBytes += 5; // ListBegin
    for (byte[] encodedSpan : encodedSpans) {
      sizeInBytes += SpanToLogEntry.sizeOfLogEntry(category, encodedSpan);
    }
    sizeInBytes += 1; // FieldStop
    return sizeInBytes;
  }

  private void sendMessage(List<byte[]> encodedSpans) throws TException {
    prot.writeMessageBegin(new TMessage("Log", TMessageType.CALL, ++seqid_));
    prot.writeFieldBegin(MESSAGES_FIELD_DESC);
    prot.writeListBegin(new TList(TType.STRUCT, encodedSpans.size()));
    for (byte[] encodedSpan : encodedSpans) SpanToLogEntry.write(category, encodedSpan, prot);
    prot.writeFieldStop();
  }

  private ResultCode parseResponse() throws TException {
    ResultCode result = null;
    prot.readStructBegin();
    TField schemeField;
    while ((schemeField = prot.readFieldBegin()).type != TType.STOP) {
      if (schemeField.id == 0 /* SUCCESS */ && schemeField.type == TType.I32) {
        result = ResultCode.findByValue(prot.readI32());
      } else {
        TProtocolUtil.skip(prot, schemeField.type);
      }
    }
    return result;
  }

  @Override public void close() throws IOException {
    socket.close();
  }
}
