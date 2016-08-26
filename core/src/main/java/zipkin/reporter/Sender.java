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
package zipkin.reporter;

import java.util.List;
import zipkin.Component;
import zipkin.collector.Collector;

/**
 * Sends a list of encoded spans to a transport such as http or Kafka. Usually, this involves
 * encoding them into a message and enqueueing them for transport over http or Kafka. The typical
 * end recipient is a zipkin {@link Collector}.
 *
 * <p>Those looking to initialize eagerly should call {@link #check()}. This can be used to reduce
 * latency on the first send operation, or to fail fast.
 *
 * <p><em>Implementation notes</em>
 *
 * <p>The parameter is a list of encoded spans as opposed to an encoded message. This allows
 * implementations flexibility on how to {@link MessageEncoder#encode(List) encode spans into a
 * message}. For example, a large span might need to be sent as a separate message to avoid kafka
 * limits. Also, logging transports like scribe will likely write each span as a separate log line.
 *
 * <p>This accepts a list of @link Encoder#encode(Object) encoded buffers}, such as a byte arrays,
 * as opposed a list of spans like {@link zipkin.Span}. This allows senders to be re-usable as model
 * shapes change. This also allows them to use their most natural item type. For example, okhttp
 * would more naturally send http bodies as okio buffers.
 *
 * <p>If performance is critical, {@link Encoding#THRIFT thrift encoding} is most efficient,
 * sometimes an order of magnitude faster than json.
 *
 * @param <B> buffer holding the {@link Encoder#encode(Object) encoded span}. For example "byte[]"
 */
public interface Sender<B> extends Component {

  interface MessageEncoding {
    /** Returns the encoding this sender requires. */
    Encoding encoding();

    /**
     * Before invoking {@link #sendSpans(List, Callback)}, callers must consider encoding overhead,
     * so as to not exceed {@link Sender#messageMaxBytes()}.
     *
     * <p>Ex. json encoding is typically 2 (for open-closing the array) + spanCount - 1 (for commas)
     */
    int overheadInBytes(int spanCount);
  }

  /**
   * Maximum bytes sendable per message including {@link MessageEncoding#overheadInBytes(int)
   * overhead}.
   */
  int messageMaxBytes();

  MessageEncoding messageEncoding();

  /**
   * Sends a list of encoded spans to a transport such as http or Kafka.
   *
   * <p>Note: Eventhough there's a callback, there's no guarantee implementations won't block.
   * Accordingly, this method should not be called on the operation being measured's thread.
   *
   * @param encodedSpans list of encoded spans.
   * @param callback signals either completion or failure
   */
  void sendSpans(List<B> encodedSpans, Callback callback);
}
