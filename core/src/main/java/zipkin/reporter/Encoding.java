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

import static zipkin.internal.Util.checkNotNull;

public enum Encoding {

  JSON,
  /** TBinaryProtocol big-endian encoding */
  THRIFT;

  /**
   * Returns the encoding associated with this message or throws {@linkplain
   * UnsupportedOperationException}.
   *
   * <p>In TBinaryProtocol encoding, the first byte is the TType, in a range 0-16. If the first byte
   * isn't in that range, it isn't a thrift.
   *
   * <p>When byte(0) == '{' (123), assume it is a json-encoded span
   *
   * <p>When byte(0) <= 16, assume it is a TBinaryProtocol-encoded thrift
   */
  public static Encoding detectFromSpan(byte[] span) {
    checkNotNull(span, "span");
    if (span[0] == '{') {
      return JSON;
    } else if (span[0] > 0 && span[0] <= 12) {
      return THRIFT;
    } else {
      throw new UnsupportedOperationException("Can only detect json or thrift");
    }
  }
}
