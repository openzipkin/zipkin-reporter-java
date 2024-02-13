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

import zipkin2.reporter.internal.Nullable;

class IpWriter {
  /** Originally from zipkin2.Endpoint.getIpv4Bytes */
  @Nullable static void writeIpv4Bytes(WriteBuffer b, @Nullable String ipv4) {
    if (ipv4 == null) return;
    for (int i = 0, len = ipv4.length(); i < len; ) {
      char ch = ipv4.charAt(i++);
      int octet = ch - '0';
      if (i == len || (ch = ipv4.charAt(i++)) == '.') {
        // then we have a single digit octet
        b.writeByte((byte) octet);
        continue;
      }
      // push the decimal
      octet = (octet * 10) + (ch - '0');
      if (i == len || (ch = ipv4.charAt(i++)) == '.') {
        // then we have a two digit octet
        b.writeByte((byte) octet);
        continue;
      }
      // otherwise, we have a three digit octet
      octet = (octet * 10) + (ch - '0');
      b.writeByte((byte) octet);
      i++; // skip the dot
    }
  }

  // Begin code adapted from com.google.common.net.InetAddresses.textToNumericFormatV6 33
  static final int IPV6_PART_COUNT = 8;
  static final char IPV6_DELIMITER = ':';
  @Nullable static void writeIpv6Bytes(WriteBuffer b, @Nullable String ipString) {
    if (ipString == null) return;
    // An address can have [2..8] colons.
    int delimiterCount = countColon(ipString);
    if (delimiterCount < 2 || delimiterCount > IPV6_PART_COUNT) {
      return;
    }
    int partsSkipped = IPV6_PART_COUNT - (delimiterCount + 1); // estimate; may be modified later
    boolean hasSkip = false;
    // Scan for the appearance of ::, to mark a skip-format IPV6 string and adjust the partsSkipped
    // estimate.
    for (int i = 0; i < ipString.length() - 1; i++) {
      if (ipString.charAt(i) == IPV6_DELIMITER && ipString.charAt(i + 1) == IPV6_DELIMITER) {
        if (hasSkip) {
          return; // Can't have more than one ::
        }
        hasSkip = true;
        partsSkipped++; // :: means we skipped an extra part in between the two delimiters.
        if (i == 0) {
          partsSkipped++; // Begins with ::, so we skipped the part preceding the first :
        }
        if (i == ipString.length() - 2) {
          partsSkipped++; // Ends with ::, so we skipped the part after the last :
        }
      }
    }
    if (ipString.charAt(0) == IPV6_DELIMITER && ipString.charAt(1) != IPV6_DELIMITER) {
      return; // ^: requires ^::
    }
    if (ipString.charAt(ipString.length() - 1) == IPV6_DELIMITER
      && ipString.charAt(ipString.length() - 2) != IPV6_DELIMITER) {
      return; // :$ requires ::$
    }
    if (hasSkip && partsSkipped <= 0) {
      return; // :: must expand to at least one '0'
    }
    if (!hasSkip && delimiterCount + 1 != IPV6_PART_COUNT) {
      return; // Incorrect number of parts
    }

    try {
      // Iterate through the parts of the ip string.
      // Invariant: start is always the beginning of a hextet, or the second ':' of the skip
      // sequence "::"
      int start = 0;
      if (ipString.charAt(0) == IPV6_DELIMITER) {
        start = 1;
      }
      while (start < ipString.length()) {
        int end = ipString.indexOf(IPV6_DELIMITER, start);
        if (end == -1) {
          end = ipString.length();
        }
        if (ipString.charAt(start) == IPV6_DELIMITER) {
          // expand zeroes
          for (int i = 0; i < partsSkipped; i++) {
            b.writeShort((short) 0);
          }

        } else {
          b.writeShort(parseHextet(ipString, start, end));
        }
        start = end + 1;
      }
    } catch (NumberFormatException ignored) {
    }
  }

  static int countColon(CharSequence sequence) {
    int count = 0;
    for (int i = 0; i < sequence.length(); i++) {
      if (sequence.charAt(i) == ':') {
        count++;
      }
    }
    return count;
  }

  // Parse a hextet out of the ipString from start (inclusive) to end (exclusive)
  static short parseHextet(String ipString, int start, int end) {
    // Note: we already verified that this string contains only hex digits.
    int length = end - start;
    if (length <= 0 || length > 4) {
      throw new NumberFormatException();
    }
    int hextet = 0;
    for (int i = start; i < end; i++) {
      hextet = hextet << 4;
      hextet |= Character.digit(ipString.charAt(i), 16);
    }
    return (short) hextet;
  }
  // End code from com.google.common.net.InetAddresses 33
}
