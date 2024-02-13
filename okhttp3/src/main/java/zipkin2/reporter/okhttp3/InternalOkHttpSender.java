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
import java.util.List;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import okhttp3.Call;
import okhttp3.Dispatcher;
import okhttp3.HttpUrl;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okhttp3.ResponseBody;
import okio.Buffer;
import okio.BufferedSink;
import okio.BufferedSource;
import okio.GzipSink;
import okio.GzipSource;
import okio.Okio;
import zipkin2.reporter.Component;
import zipkin2.reporter.Encoding;
import zipkin2.reporter.HttpSender;

/**
 * We have to nest this class until v4 when {@linkplain OkHttpSender} no longer needs to extend
 * {@linkplain Component}.
 */
final class InternalOkHttpSender extends HttpSender<HttpUrl, RequestBody> {
  final OkHttpClient client;
  final RequestBodyMessageEncoder encoder;
  final Encoding encoding;
  final int messageMaxBytes, maxRequests;
  final boolean compressionEnabled;

  InternalOkHttpSender(OkHttpSender.Builder builder) {
    super(builder.encoding, builder.endpointSupplierFactory, builder.endpoint);
    encoding = builder.encoding;
    encoder = RequestBodyMessageEncoder.forEncoding(encoding);
    maxRequests = builder.maxRequests;
    messageMaxBytes = builder.messageMaxBytes;
    compressionEnabled = builder.compressionEnabled;
    Dispatcher dispatcher = newDispatcher(maxRequests);

    // doing the extra "build" here prevents us from leaking our dispatcher to the builder
    client = builder.clientBuilder().build().newBuilder().dispatcher(dispatcher).build();
  }

  @Override public int messageMaxBytes() {
    return messageMaxBytes;
  }

  @Override protected HttpUrl newEndpoint(String endpoint) {
    HttpUrl parsed = HttpUrl.parse(endpoint);
    if (parsed == null) throw new IllegalArgumentException("invalid POST url: " + endpoint);
    return parsed;
  }

  @Override protected RequestBody newBody(List<byte[]> encodedSpans) {
    return encoder.encode(encodedSpans);
  }

  @Override protected void postSpans(HttpUrl endpoint, RequestBody body) throws IOException {
    Request request = newRequest(endpoint, body);
    Call call = client.newCall(request);
    parseResponse(call.execute());
  }

  Request newRequest(HttpUrl endpoint, RequestBody body)
    throws IOException {
    Request.Builder request = new Request.Builder().url(endpoint);
    // Amplification can occur when the Zipkin endpoint is proxied, and the proxy is instrumented.
    // This prevents that in proxies, such as Envoy, that understand B3 single format,
    request.addHeader("b3", "0");
    if (compressionEnabled) {
      request.addHeader("Content-Encoding", "gzip");
      Buffer gzipped = new Buffer();
      BufferedSink gzipSink = Okio.buffer(new GzipSink(gzipped));
      body.writeTo(gzipSink);
      gzipSink.close();
      body = new BufferRequestBody(body.contentType(), gzipped);
    }
    request.post(body);
    return request.build();
  }

  static final class BufferRequestBody extends RequestBody {
    final MediaType contentType;
    final Buffer body;

    BufferRequestBody(MediaType contentType, Buffer body) {
      this.contentType = contentType;
      this.body = body;
    }

    @Override public long contentLength() {
      return body.size();
    }

    @Override public MediaType contentType() {
      return contentType;
    }

    @Override public void writeTo(BufferedSink sink) throws IOException {
      sink.write(body, body.size());
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

  /** Waits up to a second for in-flight requests to finish before cancelling them */
  @Override protected void doClose() {
    Dispatcher dispatcher = client.dispatcher();
    dispatcher.executorService().shutdown();
    try {
      if (!dispatcher.executorService().awaitTermination(1, TimeUnit.SECONDS)) {
        dispatcher.cancelAll();
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  static Dispatcher newDispatcher(int maxRequests) {
    // bound the executor so that we get consistent performance
    ThreadPoolExecutor dispatchExecutor =
      new ThreadPoolExecutor(0, maxRequests, 60, TimeUnit.SECONDS,
        // Using a synchronous queue means messages will send immediately until we hit max
        // in-flight requests. Once max requests are hit, send will block the caller, which is
        // the AsyncReporter flush thread. This is ok, as the AsyncReporter has a buffer of
        // unsent spans for this purpose.
        new SynchronousQueue<Runnable>(),
        OkHttpSenderThreadFactory.INSTANCE);

    Dispatcher dispatcher = new Dispatcher(dispatchExecutor);
    dispatcher.setMaxRequests(maxRequests);
    dispatcher.setMaxRequestsPerHost(maxRequests);
    return dispatcher;
  }

  enum OkHttpSenderThreadFactory implements ThreadFactory {
    INSTANCE;

    @Override public Thread newThread(Runnable r) {
      return new Thread(r, "OkHttpSender Dispatcher");
    }
  }

  @Override public String toString() {
    return super.toString().replace("Internal", "");
  }
}
