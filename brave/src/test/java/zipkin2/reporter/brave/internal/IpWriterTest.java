/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
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
