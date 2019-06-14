/*
 * Copyright 2016-2019 The OpenZipkin Authors
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

import java.util.List;
import zipkin2.Call;
import zipkin2.CheckResult;
import zipkin2.codec.Encoding;

final class NoopSender extends Sender {

  final Encoding encoding;
  final BytesMessageEncoder messageEncoder;

  /** close is typically called from a different thread */
  volatile boolean closeCalled;

  NoopSender(Encoding encoding) {
    this.encoding = encoding;
    this.messageEncoder = BytesMessageEncoder.forEncoding(encoding);
  }

  @Override public int messageMaxBytes() {
    return Integer.MAX_VALUE;
  }

  @Override public Encoding encoding() {
    return encoding;
  }

  @Override public int messageSizeInBytes(List<byte[]> encodedSpans) {
    return encoding().listSizeInBytes(encodedSpans);
  }

  @Override public int messageSizeInBytes(int encodedSizeInBytes) {
    return encoding().listSizeInBytes(encodedSizeInBytes);
  }

  @Override public Call<Void> sendSpans(List<byte[]> encodedSpans) {
    messageEncoder.encode(encodedSpans);
    return Call.create(null);
  }

  @Override public CheckResult check() {
    return CheckResult.OK;
  }

  @Override public void close() {
    closeCalled = true;
  }

  @Override public String toString() {
    return "NoopSender";
  }
}
