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


import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.junit.*;
import org.junit.rules.ExpectedException;
import zipkin2.Call;
import zipkin2.CheckResult;
import zipkin2.Span;
import zipkin2.codec.Encoding;
import zipkin2.codec.SpanBytesDecoder;
import zipkin2.codec.SpanBytesEncoder;

import javax.jms.*;
import java.lang.IllegalStateException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;
import static zipkin2.reporter.TestObjects.CLIENT_SPAN;

/**
 * @author tianjunwei
 * @date 2019/3/20 19:36
 */

public class ActiveMQSenderTest {

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  ActiveMQSender sender;
  BrokerService broker;

  @Before
  public void open() throws Exception {

    broker = new BrokerService();
    broker.setUseJmx(false);
    broker.setBrokerName("MyBroker");
    broker.setPersistent(false);
    broker.addConnector("tcp://localhost:61616");
    broker.start();

    Thread.sleep(10000);

    sender = ActiveMQSender.newBuilder()
      .queue("zipkin").username("system").password("manager").connectionTimeout(2000)
      .addresses("tcp://localhost:61616").build();

    CheckResult check = sender.check();
    if (!check.ok()) {
      throw new AssumptionViolatedException(check.error().getMessage(), check.error());
    }
  }


  @After
  public void close() throws Exception{
    sender.close();

    broker.stop();

  }


  @Test
  public void sendsSpans() throws Exception {
    send(CLIENT_SPAN, CLIENT_SPAN).execute();

    assertThat(SpanBytesDecoder.JSON_V2.decodeList(readMessage()))
      .containsExactly(CLIENT_SPAN, CLIENT_SPAN);
  }

  @Test
  public void sendsSpans_PROTO3() throws Exception {
    sender.close();
    Thread.sleep(100);

    sender = sender.toBuilder().encoding(Encoding.PROTO3).build();

    send(CLIENT_SPAN, CLIENT_SPAN).execute();

    assertThat(SpanBytesDecoder.PROTO3.decodeList(readMessage()))
      .containsExactly(CLIENT_SPAN, CLIENT_SPAN);
  }

  @Test
  public void sendsSpansToCorrectQueue() throws Exception {
    sender.close();
    Thread.sleep(100);

    sender = sender.toBuilder().queue("zipkin").build();

    send(CLIENT_SPAN, CLIENT_SPAN).execute();

    assertThat(SpanBytesDecoder.JSON_V2.decodeList(readMessage()))
      .containsExactly(CLIENT_SPAN, CLIENT_SPAN);
  }

  @Test
  public void checkFalseWhenRabbitMQIsDown() throws Exception {
    sender.close();
    sender = sender.toBuilder().connectionTimeout(100).addresses("1.2.3.4:1213").build();

    CheckResult check = sender.check();
    assertThat(check.ok()).isFalse();
    assertThat(check.error())
      .isInstanceOf(RuntimeException.class);
  }

  @Test
  public void illegalToSendWhenClosed() throws Exception {
    thrown.expect(IllegalStateException.class);
    sender.close();

    send(CLIENT_SPAN, CLIENT_SPAN).execute();
  }

  @Test
  public void shouldCloseRabbitMQProducerOnClose() throws Exception {
    send(CLIENT_SPAN, CLIENT_SPAN).execute();

    sender.close();
    assertThat(sender.get().isClosed())
      .isTrue();
  }

  private byte[] readMessage(){
    ConnectionFactory connectionFactory;
    Connection connection = null;
    Session session = null;
    Destination destination;
    MessageConsumer messageConsumer = null;
    final AtomicReference<byte[]> result = new AtomicReference<>();
    connectionFactory = new ActiveMQConnectionFactory("", "", ActiveMQConnection.DEFAULT_BROKER_URL);
    try {
        connection  = connectionFactory.createConnection();
        connection.start();
        session=connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        destination = session.createQueue("zipkin"); //创建消息队列 用于接收发布的消息
        messageConsumer = session.createConsumer(destination);
        messageConsumer.setMessageListener(new MessageListener() {
          @Override
          public void onMessage(Message message) {
            byte [] data;
            try {
              data = new byte[(int)((BytesMessage)message).getBodyLength()];
              ((BytesMessage)message).readBytes(data);
              result.set(data);
            }catch (JMSException e){

            }
          }
        });

        Thread.sleep(10000);

    } catch (Exception e) {

    }finally {
      if(connection != null){
        try {
          connection.close();
        }catch (JMSException e){

        }

      }

      if(session != null){
        try {
          session.close();
        }catch (JMSException e){

        }
      }

      if(messageConsumer != null){
        try {
          messageConsumer.close();
        }catch (JMSException e){

        }

      }

    }



    return result.get();
  }




  /** Blocks until the callback completes to allow read-your-writes consistency during tests. */
  Call<Void> send(Span... spans) {
    SpanBytesEncoder bytesEncoder = sender.encoding() == Encoding.JSON
      ? SpanBytesEncoder.JSON_V2 : SpanBytesEncoder.PROTO3;
    return sender.sendSpans(Stream.of(spans).map(bytesEncoder::encode).collect(toList()));
  }

}
