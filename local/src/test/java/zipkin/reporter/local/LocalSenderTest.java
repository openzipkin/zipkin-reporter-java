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
package zipkin.reporter.local;

import java.util.List;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import zipkin.Span;
import zipkin.TestObjects;
import zipkin.reporter.Encoder;
import zipkin.reporter.internal.AwaitableCallback;
import zipkin.storage.InMemoryStorage;
import zipkin.storage.QueryRequest;

import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;

public class LocalSenderTest {
  @Rule public ExpectedException thrown = ExpectedException.none();

  InMemoryStorage storage = new InMemoryStorage();
  LocalSender sender = LocalSender.builder().storage(storage).build();

  @Test
  public void sendsSpans() throws Exception {
    send(TestObjects.TRACE);

    assertThat(storage.spanStore().getTraces(QueryRequest.builder().build()))
        .containsExactly(TestObjects.TRACE);
  }

  @Test
  public void check_okWhenStorageOk() throws Exception {
    assertThat(sender.check().ok)
        .isTrue();
  }

  @Test
  public void illegalToSendWhenClosed() throws Exception {
    thrown.expect(IllegalStateException.class);
    sender.close();

    send(TestObjects.TRACE);
  }

  /** Blocks until the callback completes to allow read-your-writes consistency during tests. */
  void send(List<Span> spans) {
    AwaitableCallback callback = new AwaitableCallback();
    sender.sendSpans(spans.stream().map(Encoder.THRIFT::encode).collect(toList()), callback);
    callback.await();
  }
}
