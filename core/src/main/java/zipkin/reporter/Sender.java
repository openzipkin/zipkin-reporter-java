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
import zipkin.Span;
import zipkin.collector.Collector;

/**
 * Sends a list of encoded spans to a transport such as http or Kafka. Usually, this involves {@link
 * ListEncoder#encode(List) encoding them into a message} and enqueueing them for transport over
 * http or Kafka. The typical end recipient is a zipkin {@link Collector}.
 *
 * <p>{@link Encoding#THRIFT thrift encoding} is preferred for production as it is most efficient
 * and works with all transports.
 *
 * <p>Those looking to initialize eagerly should call {@link #check()}. This can be used to reduce
 * latency on the first send operation, or to fail fast.
 *
 * <p>Implementation note: This is intentionally a list of encoded spans, as opposed to a
 * pre-composed message. This allows implementations flexibility on how to {@link ListEncoder bundle
 * spans for transport}. For example, a large span might need to be sent as a separate message to
 * avoid kafka limits. Also, logging transports like scribe will likely write each span as a
 * separate log line.
 *
 * @see SpanEncoder
 * @see ListEncoder
 */
// @FunctionalInterface
public interface Sender extends Component {

  /**
   * Sends a list of {@link SpanEncoder#encode(Span) encoded spans} to a transport such as http or
   * Kafka.
   *
   * <p>Note: Eventhough there's a callback, there's no guarantee implementations won't block.
   * Accordingly, this method should not be called on the operation being measured's thread.
   *
   * @param spans list of encoded spans.
   * @param callback signals either completion or failure
   */
  void sendSpans(List<byte[]> spans, Callback callback);
}
