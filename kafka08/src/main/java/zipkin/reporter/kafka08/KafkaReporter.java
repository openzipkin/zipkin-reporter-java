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
package zipkin.reporter.kafka08;

import com.google.auto.value.AutoValue;
import java.io.Closeable;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Executor;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import zipkin.Codec;
import zipkin.Span;
import zipkin.reporter.Reporter;

/** Reports spans to Zipkin, using a Kafka topic. */
@AutoValue
public abstract class KafkaReporter implements Reporter, Closeable {

  public static Builder builder() {
    return new AutoValue_KafkaReporter.Builder()
        .topic("zipkin")
        .executor(Runnable::run);
  }

  /**
   * @param bootstrapServers Initial set of kafka servers to connect to, rest of cluster will be
   * discovered (comma separated)
   */
  public static Builder builder(String bootstrapServers) {
    Properties props = new Properties();
    props.put("bootstrap.servers", bootstrapServers);
    props.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
    props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
    return builder().kafkaProperties(props);
  }

  @AutoValue.Builder
  public static abstract class Builder {
    /**
     * Configuration for the Kafka producer. Essential configuration properties are:
     * bootstrap.servers, key.serializer, value.serializer. For a full list of config options, see
     * http://kafka.apache.org/documentation.html#kafkaPropertiess.
     */
    public final Builder kafkaProperties(Properties kafkaProperties) {
      return producer(new KafkaProducer<>(kafkaProperties));
    }

    // not public as derived
    abstract Builder producer(KafkaProducer<byte[], byte[]> producer);

    /** Default zipkin. Sets the topic zipkin should report to. */
    public abstract Builder topic(String topic);

    /** Default calling thread. The executor used to defer kafka metadata lookups. */
    public abstract Builder executor(Executor executor);

    public abstract KafkaReporter build();

    Builder() {
    }
  }

  abstract String topic();

  abstract KafkaProducer<byte[], byte[]> producer();

  abstract Executor executor();

  /** Asynchronously sends the spans as a thrift message to the Kafka {@link #topic()}. */
  @Override public void report(List<Span> spans, final Callback callback) {
    executor().execute(() -> {
      try {
        byte[] value = Codec.THRIFT.writeSpans(spans);
        send(value, callback);
      } catch (RuntimeException | Error e) {
        callback.onError(e);
        if (e instanceof Error) throw (Error) e;
      }
    });
  }

  void send(byte[] value, Callback callback) {
    // NOTE: this blocks until the metadata server is available
    producer().send(new ProducerRecord<byte[], byte[]>(topic(), value), (metadata, exception) -> {
      if (exception == null) {
        callback.onComplete();
      } else {
        callback.onError(exception);
      }
    });
  }

  @Override public void close() {
    producer().close();
  }

  KafkaReporter() {
  }
}
