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
package zipkin2.reporter.amqp;

import org.junit.jupiter.api.Test;
import zipkin2.CheckResult;
import zipkin2.reporter.AsyncReporter;
import zipkin2.reporter.ClosedSenderException;
import zipkin2.reporter.Sender;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static zipkin2.TestObjects.CLIENT_SPAN;
import static zipkin2.reporter.amqp.ITRabbitMQSender.send;

class RabbitMQSenderTest {
  // We can be pretty certain RabbitMQ isn't running on localhost port 80
  RabbitMQSender sender = RabbitMQSender.newBuilder()
      .connectionTimeout(100).addresses("localhost:80").build();

  @Test void checkFalseWhenRabbitMQIsDown() {
    CheckResult check = sender.check();
    assertThat(check.ok()).isFalse();
    assertThat(check.error())
        .isInstanceOf(RuntimeException.class);
  }

  @Test void illegalToSendWhenClosed() throws Exception {
    sender.close();

    assertThatThrownBy(() -> send(sender, CLIENT_SPAN, CLIENT_SPAN))
        .isInstanceOf(ClosedSenderException.class);
  }

  /**
   * The output of toString() on {@link Sender} implementations appears in thread names created by
   * {@link AsyncReporter}. Since thread names are likely to be exposed in logs and other monitoring
   * tools, care should be taken to ensure the toString() output is a reasonable length and does not
   * contain sensitive information.
   */
  @Test void toStringContainsOnlySummaryInformation() {
    assertThat(sender).hasToString(
        "RabbitMQSender{addresses=[localhost:80], queue=zipkin}"
    );
  }
}
