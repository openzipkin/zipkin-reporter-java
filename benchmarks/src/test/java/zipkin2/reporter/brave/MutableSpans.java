/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin2.reporter.brave;

import brave.Span;
import brave.handler.MutableSpan;

class MutableSpans {
  static MutableSpan newServerSpan() {
    MutableSpan span = new MutableSpan();
    span.name("get /");
    span.kind(Span.Kind.SERVER);
    span.remoteIpAndPort("::1", 63596);
    span.startTimestamp(1533706251750057L);
    span.finishTimestamp(1533706251935296L);
    span.tag("http.method", "GET");
    span.tag("http.path", "/");
    span.tag("mvc.controller.class", "Frontend");
    span.tag("mvc.controller.method", "callBackend");
    return span;
  }

  static MutableSpan newBigClientSpan() {
    MutableSpan span = new MutableSpan();
    span.name("getuserinfobyaccesstoken");
    span.kind(Span.Kind.CLIENT);
    span.remoteServiceName("abasdasgad.hsadas.ism");
    span.remoteIpAndPort("219.235.216.11", 0);
    span.startTimestamp(1533706251750057L);
    span.finishTimestamp(1533706251935296L);
    span.tag("address.local", "/10.1.2.3:59618");
    span.tag("address.remote", "abasdasgad.hsadas.ism/219.235.216.11:8080");
    span.tag("http.host", "abasdasgad.hsadas.ism");
    span.tag("http.method", "POST");
    span.tag("http.path", "/thrift/shopForTalk");
    span.tag("http.status_code", "200");
    span.tag("http.url", "tbinary+h2c://abasdasgad.hsadas.ism/thrift/shopForTalk");
    span.tag("error", "true");
    span.tag("instanceId", "line-wallet-api");
    span.tag("phase", "beta");
    span.tag("siteId", "shop");
    span.error(new RuntimeException("ice cream"));
    return span;
  }
}
