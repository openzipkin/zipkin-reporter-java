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


import org.junit.*;
import org.junit.rules.ExpectedException;
import zipkin2.Call;
import zipkin2.CheckResult;
import zipkin2.Span;
import zipkin2.codec.Encoding;
import zipkin2.codec.SpanBytesEncoder;

import java.io.IOException;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;
import static zipkin2.reporter.TestObjects.CLIENT_SPAN;

/**
 * @author tianjunwei
 * @date 2019/3/20 19:36
 */

public class ActiveMQSenderTest {

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  ActiveMQSender sender;

  @Before
  public void open() throws Exception {
    sender = ActiveMQSender.newBuilder()
      .queue("zipkin-test1").username("system").password("manager").connectionTimeout(10)
      .addresses("tcp://localhost:61616").build();

    CheckResult check = sender.check();
    if (!check.ok()) {
      throw new AssumptionViolatedException(check.error().getMessage(), check.error());
    }
  }

  @Test
  public void sendsSpans() throws Exception {
    send(CLIENT_SPAN, CLIENT_SPAN).execute();

  }


  @After
  public void close() throws IOException {
    sender.close();
  }


  /** Blocks until the callback completes to allow read-your-writes consistency during tests. */
  Call<Void> send(Span... spans) {
    SpanBytesEncoder bytesEncoder = sender.encoding() == Encoding.JSON
      ? SpanBytesEncoder.JSON_V2 : SpanBytesEncoder.PROTO3;
    return sender.sendSpans(Stream.of(spans).map(bytesEncoder::encode).collect(toList()));
  }

}
