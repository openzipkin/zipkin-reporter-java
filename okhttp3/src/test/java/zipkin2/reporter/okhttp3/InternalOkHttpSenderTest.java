/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
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
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class InternalOkHttpSenderTest {
  @Test void parseResponse_closesBody() throws Exception {

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

    InternalOkHttpSender.parseResponse(response);

    assertThat(closed.get()).isTrue();
  }
}
