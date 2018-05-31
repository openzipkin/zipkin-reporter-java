/*
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
package zipkin2.reporter;

import java.util.Collections;
import java.util.List;
import zipkin2.Call;
import zipkin2.Component;
import zipkin2.codec.BytesEncoder;
import zipkin2.codec.Encoding;

/**
 * Sends a list of encoded spans to a transport such as http or Kafka. Usually, this involves
 * encoding them into a message and enqueueing them for transport over http or Kafka. The typical
 * end recipient is a zipkin collector.
 *
 * <p>Unless mentioned otherwise, senders are not thread-safe. They were designed to be used by
 * {@link AsyncReporter}, which has a single reporting thread.
 *
 * <p>Those looking to initialize eagerly should call {@link #check()}. This can be used to reduce
 * latency on the first send operation, or to fail fast.
 *
 * <p><em>Implementation notes</em>
 *
 * <p>The parameter is a list of encoded spans as opposed to an encoded message. This allows
 * implementations flexibility on how to encode spans into a message. For example, a large span
 * might need to be sent as a separate message to avoid kafka limits. Also, logging transports like
 * scribe will likely write each span as a separate log line.
 *
 * <p>This accepts a list of {@link BytesEncoder#encode(Object) encoded spans}, as opposed a list of
 * spans like {@link zipkin2.Span}. This allows senders to be re-usable as model shapes change. This
 * also allows them to use their most natural message type. For example, kafka would more naturally
 * send messages as byte arrays.
 */
public abstract class Sender extends Component {

  /** Returns the encoding this sender requires spans to have. */
  public abstract Encoding encoding();

  /**
   * Maximum bytes sendable per message including overhead. This can be calculated using {@link
   * #messageSizeInBytes(List)}
   */
  public abstract int messageMaxBytes();

  /**
   * Before invoking {@link Sender#sendSpans(List)}, callers must consider message overhead, which
   * might be more than encoding overhead. This is used to not exceed {@link
   * Sender#messageMaxBytes()}.
   *
   * <p>Note this is not always {@link Encoding#listSizeInBytes(List)}, as some senders have
   * inefficient list encoding. For example, Scribe base64's then tags each span with a category.
   */
  public abstract int messageSizeInBytes(List<byte[]> encodedSpans);

  /**
   * Like {@link #messageSizeInBytes(List)}, except for a single-span. This is used to ensure a span
   * is never accepted that can never be sent.
   *
   * <p>Always override this, which is only abstract as added after version 2.0
   *
   * @param encodedSizeInBytes the {@link BytesEncoder#sizeInBytes(Object) encoded size} of a span
   * @since 2.2
   */
  public int messageSizeInBytes(int encodedSizeInBytes) {
    return messageSizeInBytes(Collections.singletonList(new byte[encodedSizeInBytes]));
  }

  /**
   * Sends a list of encoded spans to a transport such as http or Kafka.
   *
   * @param encodedSpans list of encoded spans.
   * @throws IllegalStateException if {@link #close() close} was called.
   */
  public abstract Call<Void> sendSpans(List<byte[]> encodedSpans);
}
