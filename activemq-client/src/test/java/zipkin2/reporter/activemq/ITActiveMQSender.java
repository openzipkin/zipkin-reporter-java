/*
 * Copyright 2015-2019 The OpenZipkin Authors
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
import java.util.stream.Stream;
import javax.jms.BytesMessage;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.junit.EmbeddedActiveMQBroker;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TestName;
import zipkin2.Call;
import zipkin2.CheckResult;
import zipkin2.Span;
import zipkin2.codec.Encoding;
import zipkin2.codec.SpanBytesDecoder;
import zipkin2.codec.SpanBytesEncoder;
import zipkin2.reporter.AsyncReporter;
import zipkin2.reporter.Sender;

import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;
import static zipkin2.TestObjects.CLIENT_SPAN;

public class ITActiveMQSender {
  @ClassRule public static EmbeddedActiveMQBroker activemq = new EmbeddedActiveMQBroker();
  @Rule public TestName testName = new TestName();
  @Rule public ExpectedException thrown = ExpectedException.none();

  ActiveMQSender sender;

  @Before public void start() {
    sender = builder().build();
  }

  @After public void stop() throws IOException {
    sender.close();
  }

  @Test public void checkPasses() {
    assertThat(sender.check().ok()).isTrue();
  }

  @Test public void checkFalseWhenBrokerIsDown() throws IOException {
    sender.close();
    ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory();
    // we can be pretty certain ActiveMQ isn't running on localhost port 80
    connectionFactory.setBrokerURL("tcp://localhost:80");
    sender = builder().connectionFactory(connectionFactory).build();

    CheckResult check = sender.check();
    assertThat(check.ok()).isFalse();
    assertThat(check.error()).isInstanceOf(IOException.class);
  }

  @Test public void sendFailsWithInvalidActiveMqServer() throws Exception {
    sender.close();
    ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory();
    // we can be pretty certain ActiveMQ isn't running on localhost port 80
    connectionFactory.setBrokerURL("tcp://localhost:80");
    sender = builder().connectionFactory(connectionFactory).build();

    thrown.expect(IOException.class);
    thrown.expectMessage("Unable to establish connection to ActiveMQ broker: Connection refused");
    send(CLIENT_SPAN, CLIENT_SPAN).execute();
  }

  @Test public void sendsSpans() throws Exception {
    send(CLIENT_SPAN, CLIENT_SPAN).execute();

    assertThat(SpanBytesDecoder.JSON_V2.decodeList(readMessage()))
      .containsExactly(CLIENT_SPAN, CLIENT_SPAN);
  }

  @Test public void sendsSpans_PROTO3() throws Exception {
    sender.close();
    sender = builder().encoding(Encoding.PROTO3).build();

    send(CLIENT_SPAN, CLIENT_SPAN).execute();

    assertThat(SpanBytesDecoder.PROTO3.decodeList(readMessage()))
      .containsExactly(CLIENT_SPAN, CLIENT_SPAN);
  }

  @Test public void sendsSpans_THRIFT() throws Exception {
    sender.close();
    sender = builder().encoding(Encoding.THRIFT).build();

    send(CLIENT_SPAN, CLIENT_SPAN).execute();

    assertThat(SpanBytesDecoder.THRIFT.decodeList(readMessage()))
      .containsExactly(CLIENT_SPAN, CLIENT_SPAN);
  }

  @Test public void sendsSpansToCorrectQueue() throws Exception {
    sender.close();
    sender = builder().queue("customzipkinqueue").build();

    send(CLIENT_SPAN, CLIENT_SPAN).execute();

    assertThat(SpanBytesDecoder.JSON_V2.decodeList(readMessage()))
      .containsExactly(CLIENT_SPAN, CLIENT_SPAN);
  }

  @Test public void illegalToSendWhenClosed() throws Exception {
    thrown.expect(IllegalStateException.class);
    sender.close();

    send(CLIENT_SPAN, CLIENT_SPAN).execute();
  }

  /**
   * The output of toString() on {@link Sender} implementations appears in thread names created by
   * {@link AsyncReporter}. Since thread names are likely to be exposed in logs and other monitoring
   * tools, care should be taken to ensure the toString() output is a reasonable length and does not
   * contain sensitive information.
   */
  @Test public void toStringContainsOnlySummaryInformation() {
    assertThat(sender).hasToString(String.format("ActiveMQSender{brokerURL=%s, queue=%s}",
      activemq.getVmURL(), testName.getMethodName())
    );
  }

  Call<Void> send(Span... spans) {
    SpanBytesEncoder bytesEncoder;
    switch (sender.encoding()) {
      case JSON:
        bytesEncoder = SpanBytesEncoder.JSON_V2;
        break;
      case THRIFT:
        bytesEncoder = SpanBytesEncoder.THRIFT;
        break;
      case PROTO3:
        bytesEncoder = SpanBytesEncoder.PROTO3;
        break;
      default:
        throw new UnsupportedOperationException("encoding: " + sender.encoding());
    }
    return sender.sendSpans(Stream.of(spans).map(bytesEncoder::encode).collect(toList()));
  }

  private byte[] readMessage() throws Exception {
    BytesMessage message = activemq.peekBytesMessage(sender.lazyInit.queue);
    byte[] result = new byte[(int) message.getBodyLength()];
    message.readBytes(result);
    return result;
  }

  ActiveMQSender.Builder builder() {
    return ActiveMQSender.newBuilder()
      .connectionFactory(activemq.createConnectionFactory())
      // prevent test flakes by having each run in an individual queue
      .queue(testName.getMethodName());
  }
}
