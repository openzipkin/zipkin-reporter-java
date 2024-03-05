/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin2.reporter.brave;

import brave.Span;
import brave.handler.MutableSpan;
import java.util.Map;
import zipkin2.Annotation;
import zipkin2.Endpoint;
import zipkin2.TestObjects;

public class ZipkinSpanConverter {
  public static final MutableSpan CLIENT_SPAN = toMutableSpan(TestObjects.CLIENT_SPAN);

  public static MutableSpan toMutableSpan(zipkin2.Span zSpan) {
    MutableSpan span = new MutableSpan();
    span.traceId(zSpan.traceId());
    span.parentId(zSpan.parentId());
    span.id(zSpan.id());
    span.name(zSpan.name());
    switch (zSpan.kind()) {
      case CLIENT:
        span.kind(Span.Kind.CLIENT);
        break;
      case SERVER:
        span.kind(Span.Kind.SERVER);
        break;
      case PRODUCER:
        span.kind(Span.Kind.PRODUCER);
        break;
      case CONSUMER:
        span.kind(Span.Kind.CONSUMER);
        break;
    }
    span.localServiceName(zSpan.localServiceName());
    span.localIp(maybeIp(zSpan.localEndpoint()));
    span.localPort(maybePort(zSpan.localEndpoint()));
    span.remoteServiceName(zSpan.remoteServiceName());
    span.remoteIpAndPort(maybeIp(zSpan.remoteEndpoint()), maybePort(zSpan.remoteEndpoint()));
    span.startTimestamp(zSpan.timestampAsLong());
    span.finishTimestamp(zSpan.timestampAsLong() + zSpan.durationAsLong());
    for (Annotation a : zSpan.annotations()) {
      span.annotate(a.timestamp(), a.value());
    }
    for (Map.Entry<String, String> t : zSpan.tags().entrySet()) {
      span.tag(t.getKey(), t.getValue());
    }
    if (Boolean.TRUE.equals(zSpan.debug())) {
      span.setDebug();
    }
    if (Boolean.TRUE.equals(zSpan.shared())) {
      span.setShared();
    }
    return span;
  }

  static String maybeIp(Endpoint endpoint) {
    return endpoint != null ? endpoint.ipv6() != null ? endpoint.ipv6() : endpoint.ipv4() : null;
  }

  static int maybePort(Endpoint endpoint) {
    return endpoint != null ? endpoint.portAsInt() : 0;
  }
}
