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

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.PurgeQueueRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.elasticmq.rest.sqs.SQSRestServer;
import org.elasticmq.rest.sqs.SQSRestServerBuilder;
import org.junit.rules.ExternalResource;
import zipkin.Codec;
import zipkin.Span;

import static java.util.Collections.singletonList;


class AmazonSqsRule extends ExternalResource {

  private SQSRestServer server;
  private AmazonSQSClient client;
  private String queueUrl;

  AmazonSqsRule() {}

  public AmazonSqsRule start(int httpPort) {
    if (server == null) {
      server = SQSRestServerBuilder.withPort(httpPort).start();
      server.waitUntilStarted();
    }

    if (client == null) {
      client = new AmazonSQSClient(new BasicAWSCredentials("x", "x"));
      client.setEndpoint(String.format("http://localhost:%d", httpPort));
      queueUrl = client.createQueue("zipkin").getQueueUrl();
    }
    return this;
  }

  public String queueUrl() {
    return queueUrl;
  }

  public void shutdown() {
    server.stopAndWait();
  }

  @Override protected void before() {
    if (client != null && queueUrl != null) {
      client.purgeQueue(new PurgeQueueRequest(queueUrl));
    }
  }

  @Override protected void after() {
    if (server != null) {
      server.stopAndWait();
    }
  }

  public int queueCount() {
    String count = client.getQueueAttributes(queueUrl, singletonList("ApproximateNumberOfMessages"))
        .getAttributes()
        .get("ApproximateNumberOfMessages");

    return Integer.valueOf(count);
  }

  public List<Span> getTraces() {
    return getTraces(false);
  }

  public List<Span> getTraces(boolean delete) {

    Stream<Span> spans = Stream.empty();

    ReceiveMessageResult result = client.receiveMessage(queueUrl);

    while(result != null && result.getMessages().size() > 0) {

      spans = Stream.concat(spans,
          result.getMessages().stream()
          .filter(m -> m.getMessageAttributes().containsKey("z"))
          .flatMap(
              m -> fromBytes(m.getMessageAttributes().get("z").getBinaryValue().array()).stream()
          )
      );

      result = client.receiveMessage(queueUrl);

      if (delete) {
        List<DeleteMessageRequest> deletes = result.getMessages().stream()
            .map(m -> new DeleteMessageRequest(queueUrl, m.getReceiptHandle()))
            .collect(Collectors.toList());
        deletes.forEach(d -> client.deleteMessage(d));
      }
    }

    return spans.collect(Collectors.toList());
  }

  private static List<Span> fromBytes(byte[] bytes) {
    if (bytes[0] == '[') {
      return Codec.JSON.readSpans(bytes);
    } else if (bytes[0] == 12 /* TType.STRUCT */) {
      return Codec.THRIFT.readSpans(bytes);
    } else {
      return Collections.singletonList(Codec.THRIFT.readSpan(bytes));
    }
  }
}
