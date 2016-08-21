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
package zipkin.reporter.internal;

import java.util.List;
import zipkin.reporter.ListEncoder;

public final class JsonListEncoder implements ListEncoder {

  @Override public byte[] encode(List<byte[]> values) {
    int sizeOfArray = 2;
    int length = values.size();
    for (int i = 0; i < length; ) {
      sizeOfArray += values.get(i++).length;
      if (i < length) sizeOfArray++;
    }

    byte[] buf = new byte[sizeOfArray];
    int pos = 0;
    buf[pos++] = '[';
    for (int i = 0; i < length; ) {
      byte[] v = values.get(i++);
      System.arraycopy(v, 0, buf, pos, v.length);
      pos += v.length;
      if (i < length) buf[pos++] = ',';
    }
    buf[pos] = ']';
    return buf;
  }
}
