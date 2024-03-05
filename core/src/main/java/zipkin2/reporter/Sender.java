/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin2.reporter;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * Sends a list of encoded spans to a transport such as http or Kafka. Usually, this involves
 * encoding them into a message and enqueueing them for transport over http or Kafka. The typical
 * end recipient is a zipkin collector.
 *
 * <p>Unless mentioned otherwise, senders are not thread-safe. They were designed to be used by a
 * single reporting thread.
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
 * spans like {@code zipkin2.Span}. This allows senders to be re-usable as model shapes change. This
 * also allows them to use their most natural message type. For example, kafka would more naturally
 * send messages as byte arrays.
 *
 * @since 3.0
 * @deprecated since 3.2, use {@link BytesMessageSender} instead. This will be removed in v4.0.
 */
@Deprecated
public abstract class Sender extends Component implements BytesMessageSender {

  /** {@inheritDoc} */
  @Override public int messageSizeInBytes(int encodedSizeInBytes) {
    return messageSizeInBytes(Collections.singletonList(new byte[encodedSizeInBytes]));
  }

  /**
   * Sends a list of encoded spans to a transport such as http or Kafka.
   *
   * @param encodedSpans list of encoded spans.
   * @throws IllegalStateException if {@link #close() close} was called.
   * @deprecated since 3.2, use {@link BytesMessageSender} instead. This will be removed in v4.0.
   */
  @Deprecated
  public abstract Call<Void> sendSpans(List<byte[]> encodedSpans);

  /** {@inheritDoc} */
  @Override public void send(List<byte[]> encodedSpans) throws IOException {
    sendSpans(encodedSpans).execute();
  }
}
