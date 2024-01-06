/*
 * Copyright 2016-2024 The OpenZipkin Authors
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

import java.io.IOException;
import okhttp3.Response;
import okhttp3.ResponseBody;
import okio.BufferedSource;
import okio.GzipSource;
import okio.Okio;
import zipkin2.reporter.Call;
import zipkin2.reporter.Callback;

final class HttpCall extends Call<Void> {

  final okhttp3.Call call;

  HttpCall(okhttp3.Call call) {
    this.call = call;
  }

  @Override public Void execute() throws IOException {
    parseResponse(call.execute());
    return null;
  }

  @Override public void enqueue(Callback<Void> delegate) {
    call.enqueue(new V2CallbackAdapter<>(delegate));
  }

  @Override public void cancel() {
    call.cancel();
  }

  @Override public boolean isCanceled() {
    return call.isCanceled();
  }

  @Override public HttpCall clone() {
    return new HttpCall(call.clone());
  }

  static class V2CallbackAdapter<V> implements okhttp3.Callback {
    final Callback<V> delegate;

    V2CallbackAdapter(Callback<V> delegate) {
      this.delegate = delegate;
    }

    @Override public void onFailure(okhttp3.Call call, IOException e) {
      delegate.onError(e);
    }

    /** Note: this runs on the {@link okhttp3.OkHttpClient#dispatcher() dispatcher} thread! */
    @Override public void onResponse(okhttp3.Call call, Response response) {
      try {
        parseResponse(response);
        delegate.onSuccess(null);
      } catch (Throwable e) {
        propagateIfFatal(e);
        delegate.onError(e);
      }
    }
  }

  static void parseResponse(Response response) throws IOException {
    ResponseBody responseBody = response.body();
    if (responseBody == null) {
      if (response.isSuccessful()) {
        return;
      } else {
        throw new RuntimeException("response failed: " + response);
      }
    }
    try {
      BufferedSource content = responseBody.source();
      if ("gzip".equalsIgnoreCase(response.header("Content-Encoding"))) {
        content = Okio.buffer(new GzipSource(responseBody.source()));
      }
      if (!response.isSuccessful()) {
        throw new RuntimeException(
          "response for " + response.request().tag() + " failed: " + content.readUtf8());
      }
    } finally {
      responseBody.close();
    }
  }
}
