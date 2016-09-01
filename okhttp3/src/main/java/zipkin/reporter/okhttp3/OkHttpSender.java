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
package zipkin.reporter.okhttp3;

import com.google.auto.value.AutoValue;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
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
import okhttp3.internal.Util;
import okio.Buffer;
import okio.BufferedSink;
import okio.GzipSink;
import okio.Okio;
import zipkin.reporter.Callback;
import zipkin.reporter.Encoding;
import zipkin.reporter.MessageEncoder;
import zipkin.reporter.Sender;

import static zipkin.internal.Util.checkArgument;
import static zipkin.internal.Util.checkNotNull;

/**
 * Reports spans to Zipkin, using its <a href="http://zipkin.io/zipkin-api/#/">POST</a> endpoint.
 */
@AutoValue
public abstract class OkHttpSender implements Sender<RequestBody> {
  /** Creates a sender that posts {@link Encoding#THRIFT thrift} messages. */
  public static OkHttpSender create(String endpoint) {
    return builder().endpoint(endpoint).build();
  }

  public static Builder builder() {
    return new AutoValue_OkHttpSender.Builder()
        .compressionEnabled(true)
        .maxRequests(64)
        .messageMaxBytes(5 * 1024 * 1024)
        .spanEncoding(Encoding.THRIFT);
  }

  @AutoValue.Builder
  public static abstract class Builder {

    /**
     * No default. The POST URL for zipkin's <a href="http://zipkin.io/zipkin-api/#/">v1 api</a>,
     * usually "http://zipkinhost:9411/api/v1/spans"
     */
    // customizable so that users can re-map /api/v1/spans ex for browser-originated traces
    public final Builder endpoint(String endpoint) {
      checkNotNull(endpoint, "endpoint ex: http://zipkinhost:9411/api/v1/spans");
      HttpUrl parsed = HttpUrl.parse(endpoint);
      checkArgument(parsed != null, "invalid post url: " + endpoint);
      return endpoint(parsed);
    }

    public abstract Builder endpoint(HttpUrl endpoint);

    /** Default true. true implies that spans will be gzipped before transport. */
    public abstract Builder compressionEnabled(boolean compressSpans);

    /** Maximum size of a message. Default 5MiB */
    public abstract Builder messageMaxBytes(int messageMaxBytes);

    /** Maximum in-flight requests. Default 64 */
    public abstract Builder maxRequests(int maxRequests);

    /**
     * Controls the "Content-Type" header and {@link MessageEncoder#encode(List) message encoder}
     * when sending spans.
     */
    public abstract Builder spanEncoding(Encoding spanEncoding);

    abstract int maxRequests();

    abstract Encoding spanEncoding();

    public final OkHttpSender build() {
      // bound the executor so that we get consistent performance
      ThreadPoolExecutor dispatchExecutor =
          new ThreadPoolExecutor(0, maxRequests(), 60, TimeUnit.SECONDS,
              new ArrayBlockingQueue<>(maxRequests()),
              Util.threadFactory("OkHttpSender Dispatcher", false));
      dispatchExecutor(dispatchExecutor);
      Dispatcher dispatcher = new Dispatcher(dispatchExecutor);
      dispatcher.setMaxRequests(maxRequests());
      dispatcher.setMaxRequestsPerHost(maxRequests());
      client(new OkHttpClient.Builder()
          .dispatcher(dispatcher).build());

      if (spanEncoding() == Encoding.JSON) {
        return encoder(RequestBodyMessageEncoder.JSON).autoBuild();
      } else if (spanEncoding() == Encoding.THRIFT) {
        return encoder(RequestBodyMessageEncoder.THRIFT).autoBuild();
      }
      throw new UnsupportedOperationException("Unsupported spanEncoding: " + spanEncoding().name());
    }

    abstract Builder dispatchExecutor(ExecutorService dispatchExecutor);

    abstract Builder client(OkHttpClient client);

    abstract Builder encoder(MessageEncoder<RequestBody> encoder);

    abstract OkHttpSender autoBuild();

    Builder() {
    }
  }

  public Builder toBuilder() {
    return new AutoValue_OkHttpSender.Builder(this);
  }

  abstract OkHttpClient client();

  abstract ExecutorService dispatchExecutor();

  abstract HttpUrl endpoint();

  abstract int maxRequests();

  abstract boolean compressionEnabled();

  @Override public abstract MessageEncoder<RequestBody> encoder();

  /** Asynchronously sends the spans as a POST to {@link #endpoint()}. */
  @Override public void sendSpans(List<byte[]> encodedSpans, Callback callback) {
    if (encodedSpans.isEmpty()) {
      callback.onComplete();
      return;
    }
    try {
      Request request = newRequest(encoder().encode(encodedSpans));
      client().newCall(request).enqueue(new CallbackAdapter(callback));
    } catch (Throwable e) {
      callback.onError(e);
      if (e instanceof Error) throw (Error) e;
    }
  }

  /** Sends an empty json message to the configured endpoint. */
  @Override public CheckResult check() {
    try {
      Request request = new Request.Builder().url(endpoint())
          .post(RequestBody.create(MediaType.parse("application/json"), "[]")).build();
      Response response = client().newCall(request).execute();
      if (!response.isSuccessful()) {
        throw new IllegalStateException("check response failed: " + response);
      }
      return CheckResult.OK;
    } catch (Exception e) {
      return CheckResult.failed(e);
    }
  }

  @Override public void close() {
    client().dispatcher().cancelAll();
    dispatchExecutor().shutdown();
  }

  Request newRequest(RequestBody body) throws IOException {
    Request.Builder request = new Request.Builder().url(endpoint());
    if (compressionEnabled()) {
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

  OkHttpSender() {
  }

  static final class BufferRequestBody extends RequestBody {
    private final MediaType contentType;
    private final Buffer body;

    public BufferRequestBody(MediaType contentType, Buffer body) {
      this.contentType = contentType;
      this.body = body;
    }

    @Override public MediaType contentType() {
      return contentType;
    }

    @Override public void writeTo(BufferedSink sink) throws IOException {
      sink.write(body, body.size());
    }
  }

  static final class CallbackAdapter implements okhttp3.Callback {
    private final Callback delegate;

    public CallbackAdapter(Callback delegate) {
      this.delegate = delegate;
    }

    @Override public void onFailure(Call call, IOException e) {
      delegate.onError(e);
    }

    @Override public void onResponse(Call call, Response response) throws IOException {
      if (response.isSuccessful()) {
        delegate.onComplete();
      } else {
        delegate.onError(new IllegalStateException("response failed: " + response));
      }
    }

    @Override public String toString() {
      return "CallbackAdapter(" + delegate + ")";
    }
  }
}
