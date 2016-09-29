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

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSCredentialsProviderChain;
import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.auth.EnvironmentVariableCredentialsProvider;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider;
import com.amazonaws.auth.SystemPropertiesCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.handlers.AsyncHandler;
import com.amazonaws.internal.StaticCredentialsProvider;
import com.amazonaws.services.sqs.AmazonSQSAsync;
import com.amazonaws.services.sqs.AmazonSQSAsyncClient;
import com.amazonaws.services.sqs.buffered.AmazonSQSBufferedAsyncClient;
import com.amazonaws.services.sqs.model.GetQueueAttributesResult;
import com.amazonaws.services.sqs.model.MessageAttributeValue;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageResult;
import com.google.auto.value.AutoValue;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.http.client.CredentialsProvider;
import zipkin.internal.LazyCloseable;
import zipkin.internal.Nullable;
import zipkin.reporter.BytesMessageEncoder;
import zipkin.reporter.Callback;
import zipkin.reporter.Encoding;
import zipkin.reporter.Sender;

import static zipkin.internal.Util.checkNotNull;

/**
 * This sends (usually TBinaryProtocol big-endian) encoded spans to an SQS queue
 */
@AutoValue
public abstract class AwsBufferedSqsSender extends LazyCloseable<AmazonSQSBufferedAsyncClient> implements Sender {

  private static AtomicInteger messageId = new AtomicInteger(0);

  public static AwsBufferedSqsSender create(String url) {
    return builder().queueUrl(url).build();
  }

  public static Builder builder() {
    return new AutoValue_AwsBufferedSqsSender.Builder()
        .encoding(Encoding.THRIFT)
        .compressionEnabled(true)
        .performCheck(false)
        .messageMaxBytes(256 * 1024 - 30); // 256KB SQS limit per message. 30 bytes for attr keys.
  }

  @AutoValue.Builder
  public static abstract class Builder {

    public abstract Builder queueUrl(String queueUrl);
    public abstract Builder compressionEnabled(boolean compressionEnabled);
    public abstract Builder dispatchExecutor(ExecutorService dispatchExecutor);
    public abstract Builder credentialsProvider(AWSCredentialsProvider credentialsProvider);
    public abstract Builder clientConfiguration(ClientConfiguration clientConfiguration);
    public abstract Builder performCheck(boolean performCheck);

    public abstract Builder encoding(Encoding encoding);

    /**
     * Maximum size of a message. SQS max message size is 256KB including attributes.
     * Default 256 less 30 bytes for message keys.
     *
     * @param messageMaxBytes
     * @return
     */
    public abstract Builder messageMaxBytes(int messageMaxBytes);

    public final AwsBufferedSqsSender build() {
      return autoBuild();
    }

    abstract AwsBufferedSqsSender autoBuild();

    Builder() {}
  }

  public Builder toBuilder() {
    return new AutoValue_AwsBufferedSqsSender.Builder(this);
  }

  abstract String queueUrl();
  abstract boolean compressionEnabled();
  abstract boolean performCheck();
  @Nullable abstract ExecutorService dispatchExecutor();
  @Nullable abstract AWSCredentialsProvider credentialsProvider();
  @Nullable abstract ClientConfiguration clientConfiguration();

  /** close is typically called from a different thread */
  private AtomicBoolean closeCalled = new AtomicBoolean(false);


  @Override public CheckResult check() {
    if (performCheck()) {
      try {
        get().getQueueAttributes(queueUrl(), Collections.singletonList("QueueArn"));
        return CheckResult.OK;
      } catch (Throwable e) {
        return CheckResult.failed(new Exception(e));
      }
    }

    return CheckResult.OK;
  }

  @Override protected AmazonSQSBufferedAsyncClient compute() {
    AWSCredentialsProvider creds = (credentialsProvider() != null) ?
        credentialsProvider() : new DefaultAWSCredentialsProviderChain();

    AmazonSQSAsyncClient client;
    if (clientConfiguration() != null && dispatchExecutor() != null) {
      client = new AmazonSQSAsyncClient(creds, clientConfiguration(), dispatchExecutor());
    } else if (clientConfiguration() != null && dispatchExecutor() == null) {
      client = new AmazonSQSAsyncClient(creds, clientConfiguration());
    } else if (clientConfiguration() == null && dispatchExecutor() != null) {
      client = new AmazonSQSAsyncClient(creds, dispatchExecutor());
    } else {
      client = new AmazonSQSAsyncClient(creds);
    }

    return new AmazonSQSBufferedAsyncClient(client);
  }

  @Override public int messageSizeInBytes(List<byte[]> encodedSpans) {
    return encoding().listSizeInBytes(encodedSpans);
  }

  @Override public void sendSpans(List<byte[]> list, Callback callback) {
    if (closeCalled.get()) throw new IllegalStateException("closed");

    checkNotNull(list, "list of encoded spans must not be null");

    byte[] encodedSpans = BytesMessageEncoder.forEncoding(encoding()).encode(list);

    get().sendMessageAsync(new SendMessageRequest(queueUrl(), "z")
        .addMessageAttributesEntry("z", new MessageAttributeValue()
            .withDataType("Binary." + encoding().name())
            .withBinaryValue(ByteBuffer.wrap(encodedSpans))), new AsyncHandlerAdapter(callback));
  }

  @Override public void close() {
    if (!closeCalled.getAndSet(true)) {
      AmazonSQSBufferedAsyncClient maybeNull = maybeNull();
      if (maybeNull != null) maybeNull.shutdown();
    }
  }

  private static ThreadFactory threadFactory(final String name, final boolean daemon) {
    return (Runnable r) -> {
      Thread result = new Thread(r, name);
          result.setDaemon(daemon);
          return result;
    };
  }

  private static class AsyncHandlerAdapter implements AsyncHandler<SendMessageRequest, SendMessageResult> {

    private final Callback callback;

    AsyncHandlerAdapter(Callback callback) {
      this.callback = callback;
    }

    @Override public void onError(Exception e) {
      callback.onError(e);
    }

    @Override
    public void onSuccess(SendMessageRequest request, SendMessageResult sendMessageResult) {
      callback.onComplete();
    }
  }
}
