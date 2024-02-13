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
package zipkin2.reporter.brave.internal;

import java.nio.ByteBuffer;
import zipkin2.reporter.internal.Nullable;

class IpParser {
  /** Originally from zipkin2.Endpoint.getIpv4Bytes */
  @Nullable static byte[] getIpv4Bytes(String ipv4) {
    byte[] result = new byte[4];
    int pos = 0;
    for (int i = 0, len = ipv4.length(); i < len; ) {
      char ch = ipv4.charAt(i++);
      int octet = ch - '0';
      if (i == len || (ch = ipv4.charAt(i++)) == '.') {
        // then we have a single digit octet
        result[pos++] = (byte) octet;
        continue;
      }
      // push the decimal
      octet = (octet * 10) + (ch - '0');
      if (i == len || (ch = ipv4.charAt(i++)) == '.') {
        // then we have a two digit octet
        result[pos++] = (byte) octet;
        continue;
      }
      // otherwise, we have a three digit octet
      octet = (octet * 10) + (ch - '0');
      result[pos++] = (byte) octet;
      i++; // skip the dot
    }
    return result;
  }

  // Begin code from com.google.common.net.InetAddresses.textToNumericFormatV6 23
  static final int IPV6_PART_COUNT = 8;

  @Nullable static byte[] getIpv6Bytes(String ipString) {
    // An address can have [2..8] colons, and N colons make N+1 parts.
    String[] parts = ipString.split(":", IPV6_PART_COUNT + 2);
    if (parts.length < 3 || parts.length > IPV6_PART_COUNT + 1) {
      return null;
    }

    // Disregarding the endpoints, find "::" with nothing in between.
    // This indicates that a run of zeroes has been skipped.
    int skipIndex = -1;
    for (int i = 1; i < parts.length - 1; i++) {
      if (parts[i].isEmpty()) {
        if (skipIndex >= 0) {
          return null; // Can't have more than one ::
        }
        skipIndex = i;
      }
    }

    int partsHi; // Number of parts to copy from above/before the "::"
    int partsLo; // Number of parts to copy from below/after the "::"
    if (skipIndex >= 0) {
      // If we found a "::", then check if it also covers the endpoints.
      partsHi = skipIndex;
      partsLo = parts.length - skipIndex - 1;
      if (parts[0].isEmpty() && --partsHi != 0) {
        return null; // ^: requires ^::
      }
      if (parts[parts.length - 1].isEmpty() && --partsLo != 0) {
        return null; // :$ requires ::$
      }
    } else {
      // Otherwise, allocate the entire address to partsHi. The endpoints
      // could still be empty, but parseHextet() will check for that.
      partsHi = parts.length;
      partsLo = 0;
    }

    // If we found a ::, then we must have skipped at least one part.
    // Otherwise, we must have exactly the right number of parts.
    int partsSkipped = IPV6_PART_COUNT - (partsHi + partsLo);
    if (!(skipIndex >= 0 ? partsSkipped >= 1 : partsSkipped == 0)) {
      return null;
    }

    // Now parse the hextets into a byte array.
    ByteBuffer rawBytes = ByteBuffer.allocate(2 * IPV6_PART_COUNT);
    try {
      for (int i = 0; i < partsHi; i++) {
        rawBytes.putShort(parseHextet(parts[i]));
      }
      for (int i = 0; i < partsSkipped; i++) {
        rawBytes.putShort((short) 0);
      }
      for (int i = partsLo; i > 0; i--) {
        rawBytes.putShort(parseHextet(parts[parts.length - i]));
      }
    } catch (NumberFormatException ex) {
      return null;
    }
    return rawBytes.array();
  }

  static short parseHextet(String ipPart) {
    // Note: we already verified that this string contains only hex digits.
    int hextet = Integer.parseInt(ipPart, 16);
    if (hextet > 0xffff) {
      throw new NumberFormatException();
    }
    return (short) hextet;
  }
  // End code from com.google.common.net.InetAddresses 23
}
