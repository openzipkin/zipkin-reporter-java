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

import java.io.IOException;
import java.util.List;

final class NoopSender implements Sender<byte[]> {

  @Override public int messageMaxBytes() {
    return Integer.MAX_VALUE;
  }

  @Override public MessageEncoding messageEncoding() {
    return MessageEncoder.THRIFT_BYTES;
  }

  @Override public void sendSpans(List<byte[]> encodedSpans, Callback callback) {
    callback.onComplete();
  }

  @Override public CheckResult check() {
    return CheckResult.OK;
  }

  @Override public void close() throws IOException {
  }

  @Override public String toString() {
    return "NoopSender";
  }
}