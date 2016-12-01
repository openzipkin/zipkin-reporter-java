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
package zipkin.reporter.awssqs;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.internal.StaticCredentialsProvider;
import java.util.List;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import zipkin.Component;
import zipkin.Span;
import zipkin.TestObjects;
import zipkin.reporter.Encoder;
import zipkin.reporter.internal.AwaitableCallback;

import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;

public class AwsBufferedSqsSenderTest {

  @Rule
  public AmazonSqsRule sqsRule = new AmazonSqsRule().start(9324);
  @Rule
  public ExpectedException thrown = ExpectedException.none();

  AwsBufferedSqsSender sender = AwsBufferedSqsSender.builder()
      .queueUrl(sqsRule.queueUrl())
      .performCheck(true)
      .credentialsProvider(new StaticCredentialsProvider(new BasicAWSCredentials("x", "x")))
      .build();

  @Test
  public void sendsSpans() throws Exception {
    send(TestObjects.TRACE);

    assertThat(sqsRule.queueCount()).isEqualTo(1);

    List<Span> traces = sqsRule.getTraces();
    List<Span> expected = TestObjects.TRACE;

    assertThat(traces.size()).isEqualTo(expected.size());
    assertThat(traces).isEqualTo(expected);
  }

  @Test
  public void checkOk() throws Exception {
    assertThat(sender.check()).isEqualTo(Component.CheckResult.OK);
  }
  @Test
  public void checkFailed() throws Exception {
    sqsRule.shutdown();
    Component.CheckResult result = sender.check();
    assertThat(result.ok).isFalse();
    assertThat(result.exception.getCause()).isInstanceOf(AmazonClientException.class);
  }


  private void send(List<Span> spans) {
    AwaitableCallback callback = new AwaitableCallback();
    sender.sendSpans(spans.stream().map(Encoder.THRIFT::encode).collect(toList()), callback);
    callback.await();
  }
}
