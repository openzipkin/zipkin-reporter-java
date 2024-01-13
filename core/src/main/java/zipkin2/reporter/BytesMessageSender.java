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
package zipkin2.reporter;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

/**
 * Sends a list of encoded spans to a transport such as HTTP or Kafka. Usually, this involves
 * encoding them into a message and enqueueing them for transport in a corresponding client library.
 * The typical end recipient is a zipkin collector.
 *
 * <p>Unless mentioned otherwise, senders are not thread-safe. They were designed to be used by a
 * single reporting thread, hence the operation is blocking
 *
 * <p>Those looking to initialize eagerly can {@link #send(List)} with an empty list. This can be
 * used to reduce latency on the first send operation, or to fail fast.
 *
 * <p><em>Implementation notes</em>
 *
 * <p>The parameter is a list of encoded spans as opposed to an encoded message. This allows
 * implementations flexibility on how to encode spans into a message. For example, a large span
 * might need to be sent as a separate message to avoid kafka limits. Also, logging transports like
 * scribe will likely write each span as a separate log line.
 *
 * <p>This accepts a list of {@link BytesEncoder#encode(Object) encoded spans}, as opposed a list of
 * spans like {@code zipkin2.Span}. This allows senders to be re-usable as model shapes change. This
 * also allows them to use their most natural message type. For example, kafka would more naturally
 * send messages as byte arrays.
 *
 * @since 3.2
 */
public interface BytesMessageSender extends Closeable {

  /**
   * Base class for implementation, which implements {@link #messageSizeInBytes(List)} and
   * {@link #messageSizeInBytes(List)} with a given {@linkplain Encoding}
   */
  abstract class Base implements BytesMessageSender {
    protected final Encoding encoding;

    protected Base(Encoding encoding) {
      this.encoding = encoding;
    }

    /** {@inheritDoc} */
    @Override public Encoding encoding() {
      return encoding;
    }

    /** {@inheritDoc} */
    @Override public int messageSizeInBytes(List<byte[]> encodedSpans) {
      return encoding.listSizeInBytes(encodedSpans);
    }

    /** {@inheritDoc} */
    @Override public int messageSizeInBytes(int encodedSizeInBytes) {
      return encoding.listSizeInBytes(encodedSizeInBytes);
    }
  }

  /** Returns the encoding this sender requires spans to have. */
  Encoding encoding();

  /**
   * Maximum bytes sendable per message including overhead. This can be calculated using {@link
   * #messageSizeInBytes(List)}
   * <p>
   * Defaults to 500KB as a conservative default. You may get better or reduced performance
   * by changing this value based on, e.g., machine size or network bandwidth in your
   * infrastructure. Finding a perfect value will require trying out different values in production,
   * but the default should work well enough in most cases.
   */
  int messageMaxBytes();

  /**
   * Before invoking {@link BytesMessageSender#send(List)}, callers must consider message overhead,
   * which might be more than encoding overhead. This is used to not exceed {@link
   * BytesMessageSender#messageMaxBytes()}.
   *
   * <p>Note this is not always {@link Encoding#listSizeInBytes(List)}, as some senders have
   * inefficient list encoding. For example, Scribe base64's then tags each span with a category.
   */
  int messageSizeInBytes(List<byte[]> encodedSpans);

  /**
   * Like {@link #messageSizeInBytes(List)}, except for a single-span. This is used to ensure a span
   * is never accepted that can never be sent.
   *
   * <p>Note this is not always {@link Encoding#listSizeInBytes(int)}, as some senders have
   * inefficient list encoding. For example, Stackdriver's proto message contains other fields.
   *
   * @param encodedSizeInBytes the {@link BytesEncoder#sizeInBytes(Object) encoded size} of a span
   */
  int messageSizeInBytes(int encodedSizeInBytes);

  /**
   * Sends a list of encoded spans to a transport such as HTTP or Kafka.
   *
   * @param encodedSpans list of encoded spans.
   * @throws IllegalStateException if {@link #close() close} was called.
   */
  void send(List<byte[]> encodedSpans) throws IOException;
}
