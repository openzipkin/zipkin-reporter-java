/**
 * Copyright 2016-2018 The OpenZipkin Authors
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
package zipkin2.reporter.okhttp3;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.concurrent.atomic.AtomicBoolean;
import okhttp3.Protocol;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.ResponseBody;
import okio.Okio;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class HttpCallTest {

  @Test public void parseResponse_closesBody() throws Exception {

    // It is difficult to prove close was called, this approach looks at an underlying stream
    AtomicBoolean closed = new AtomicBoolean();
    InputStream in = new ByteArrayInputStream(new byte[1]) {
      @Override public void close() {
        closed.set(true);
      }
    };

    // precan a response which has our inspectable body
    Response response = new Response.Builder()
        .request(new Request.Builder().url("http://localhost/foo").build())
        .protocol(Protocol.HTTP_1_1)
        .code(200)
        .message("OK")
        .body(ResponseBody.create(null, 1, Okio.buffer(Okio.source(in))))
        .build();

    HttpCall.parseResponse(response);

    assertThat(closed.get()).isTrue();
  }
}
