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

import brave.handler.MutableSpan;
import java.net.Inet6Address;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test data adapted from zipkin2.EndpointTest. We can assume the inputs are valid as
 * {@linkplain MutableSpan} already checks them.
 */
class IpWriterTest {
  @Test void writeIpv4Bytes() {
    byte[] bytes = new byte[4];
    IpWriter.writeIpv4Bytes(new WriteBuffer(bytes), "43.0.192.2");

    assertThat(bytes).containsExactly(43, 0, 192, 2);
  }

  @Test void writeIpv4Bytes_localhost() {
    byte[] bytes = new byte[4];
    IpWriter.writeIpv4Bytes(new WriteBuffer(bytes), "127.0.0.1");

    assertThat(bytes).containsExactly(127, 0, 0, 1);
  }

  @Test void writeIpv6Bytes() throws Exception {
    String ipv6 = "2001:db8::c001";

    byte[] bytes = new byte[16];
    IpWriter.writeIpv6Bytes(new WriteBuffer(bytes), ipv6);
    assertThat(bytes)
      .containsExactly(Inet6Address.getByName(ipv6).getAddress());
  }

  @Test void writeIpv6Bytes_uppercase() throws Exception {
    String ipv6 = "2001:DB8::C001";

    byte[] bytes = new byte[16];
    IpWriter.writeIpv6Bytes(new WriteBuffer(bytes), ipv6);
    assertThat(bytes)
      .containsExactly(Inet6Address.getByName(ipv6).getAddress());
  }

  @Test void writeIpv6Bytes_localhost() {
    String ipv6 = "::1";

    byte[] bytes = new byte[16];
    IpWriter.writeIpv6Bytes(new WriteBuffer(bytes), ipv6);
    assertThat(bytes)
      .containsExactly(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1);
  }
}
