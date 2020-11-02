package zipkin2.collector.scribe;

import java.net.InetSocketAddress;

public final class Access {
  // See https://github.com/openzipkin/zipkin/issues/3282
  public static int port(ScribeCollector collector) {
    return ((InetSocketAddress) collector.server.channel.localAddress()).getPort();
  }
}
