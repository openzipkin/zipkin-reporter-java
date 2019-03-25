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
package zipkin2.reporter.jms;

import org.apache.activemq.ActiveMQConnectionFactory;
import zipkin2.Call;
import zipkin2.Callback;
import zipkin2.codec.Encoding;
import zipkin2.reporter.BytesMessageEncoder;
import zipkin2.reporter.Sender;

import javax.jms.*;
import java.io.IOException;
import java.lang.IllegalStateException;
import java.util.List;

/**
 * This sends (usually json v2) encoded spans to a ActiveMQ queue.
 */

public final class ActiveMQSender extends Sender {

  private final static Object syncLock = new Object();

  final Encoding encoding;
  final int messageMaxBytes;
  final String queue;
  final BytesMessageEncoder encoder;
  final String addresses;
  final ActiveMQConnectionFactory connectionFactory;
  private volatile MessageProducer producer;
  private Session session;

  public ActiveMQSender(Builder builder){
    this.encoding = builder.encoding;
    this.messageMaxBytes = builder.messageMaxBytes;
    this.queue = builder.queue;
    this.encoder = BytesMessageEncoder.forEncoding(encoding);
    this.connectionFactory = builder.connectionFactory;
    this.addresses = builder.addresses;
  }

  public MessageProducer getProducer() throws IOException{

    if(producer == null) {
      synchronized (syncLock) {
        if (producer == null) {
          try {
            connectionFactory.setBrokerURL("failover:("+addresses+")?initialReconnectDelay=100");
            QueueConnection connection = connectionFactory.createQueueConnection();
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            connection.start();
            Destination destination = session.createQueue(queue);
            producer = session.createProducer(destination);
            closeCalled = false;
          } catch (Exception e) {
            throw new IOException(e);
          }
        }

      }
    }
    return producer;
  }

  public final Builder toBuilder() {
    return new Builder(this);
  }

  volatile boolean closeCalled;



  @Override
  public Encoding encoding() {
    return encoding;
  }

  @Override
  public int messageMaxBytes() {
    return messageMaxBytes;
  }

  @Override
  public int messageSizeInBytes(int encodedSizeInBytes) {
    return encoding.listSizeInBytes(encodedSizeInBytes);
  }

  @Override
  public int messageSizeInBytes(List<byte[]> encodedSpans) {
    return encoding.listSizeInBytes(encodedSpans);
  }

  @Override
  public Call<Void> sendSpans(List<byte[]> encodedSpans) {
    if (closeCalled) {
      throw new IllegalStateException("closed");
    }
    byte[] message = encoder.encode(encodedSpans);
    return new ActiveMQCall(message);
  }

  @Override
  public synchronized void close() throws IOException {
    if (closeCalled){
      return;
    }
    try {
      if (session != null) {
        session.close();
      }
      closeCalled = true;
    }catch (Exception e){
      throw new IOException(e);
    }

  }


  public static ActiveMQSender create(String addresses) {
    return newBuilder().addresses(addresses).build();
  }

  public static Builder newBuilder() {
    return new Builder();
  }


  public static final class Builder {

    ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory();
    String addresses;
    String queue = "zipkin";
    Encoding encoding = Encoding.JSON;
    int messageMaxBytes = 100000; // arbitrary to match kafka, messages theoretically can be 2GiB

    Builder(ActiveMQSender sender) {
      connectionFactory = sender.connectionFactory;
      addresses = sender.addresses;
      queue = sender.queue;
      encoding = sender.encoding;
      messageMaxBytes = sender.messageMaxBytes;
    }


    public Builder connectionFactory(ActiveMQConnectionFactory connectionFactory) {
      if (connectionFactory == null) {
        throw new NullPointerException("connectionFactory == null");
      }
      this.connectionFactory = connectionFactory;
      return this;
    }



    /** Comma-separated list of tcp://host:port pairs. ex "tcp://primary:61616,tcp://secondary:61616" No Default. */
    public Builder addresses(String addresses) {
      if (addresses == null) {
        throw new NullPointerException("addresses == null");
      }
      this.addresses = addresses;
      return this;
    }

    /** Queue zipkin spans will be send to. Defaults to "zipkin" */
    public Builder queue(String queue) {
      if (queue == null) {
        throw new NullPointerException("queue == null");
      }
      this.queue = queue;
      return this;
    }

    /**
     * Use this to change the encoding used in messages. Default is {@linkplain Encoding#JSON}
     *
     * <p>Note: If ultimately sending to Zipkin, version 2.8+ is required to process protobuf.
     */
    public Builder encoding(Encoding encoding) {
      if (encoding == null) {
        throw new NullPointerException("encoding == null");
      }
      this.encoding = encoding;
      return this;
    }

    /** Connection TCP establishment timeout in milliseconds. Defaults to 60 seconds */
    public Builder connectionTimeout(int connectionTimeout) {
      ((ActiveMQConnectionFactory)connectionFactory).setConnectResponseTimeout(connectionTimeout);
      return this;
    }


    /** The AMQP user name to use when connecting to the broker. Defaults to "guest" */
    public Builder username(String username) {
      ((ActiveMQConnectionFactory)connectionFactory).setUserName(username);
      return this;
    }

    /** The password to use when connecting to the broker. Defaults to "guest" */
    public Builder password(String password) {
      ((ActiveMQConnectionFactory)connectionFactory).setPassword(password);
      return this;
    }

    /** Maximum size of a message. Default 1000000. */
    public Builder messageMaxBytes(int messageMaxBytes) {
      this.messageMaxBytes = messageMaxBytes;
      return this;
    }

    public final ActiveMQSender build() {
      return new ActiveMQSender(this);
    }

    Builder() {
    }
  }



  class ActiveMQCall extends Call.Base<Void> { // RabbitMQFuture is not cancelable

    private final byte[] message;

    ActiveMQCall(byte[] message) {
      this.message = message;
    }

    @Override
    protected Void doExecute() throws IOException {
      publish();
      return null;
    }

    void publish() throws IOException {
      try {
        producer = getProducer();
        BytesMessage bytesMessage = session.createBytesMessage();
        bytesMessage.writeBytes(message);
        producer.send(bytesMessage);
      }catch (Exception e){
        throw new IOException(e);
      }

    }

    @Override
    public Call<Void> clone() {
      return new ActiveMQCall(message);
    }

    @Override
    protected void doEnqueue(Callback<Void> callback) {
      try {
        publish();
        callback.onSuccess(null);
      } catch (IOException | RuntimeException | Error e) {
        callback.onError(e);
      }

    }
  }

}
