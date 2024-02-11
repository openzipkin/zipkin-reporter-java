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
package zipkin2.reporter.okhttp3;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import okhttp3.HttpUrl;
import org.junit.jupiter.api.Test;
import zipkin2.Span;
import zipkin2.codec.SpanBytesEncoder;
import zipkin2.reporter.AsyncReporter;
import zipkin2.reporter.BytesMessageSender;
import zipkin2.reporter.ClosedSenderException;
import zipkin2.reporter.HttpEndpointSupplier;
import zipkin2.reporter.HttpEndpointSupplier.Constant;

import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static zipkin2.TestObjects.CLIENT_SPAN;

class OkHttpSenderTest {
  // We can be pretty certain Zipkin isn't listening on localhost port 19092
  OkHttpSender sender = OkHttpSender.newBuilder()
    .readTimeout(100).endpoint("http://localhost:19092").build();

  @Test void sendFailsWhenEndpointIsDown() {
    // Depending on JRE, this could be a ConnectException or a SocketException.
    // Assert IOException to satisfy both!
    assertThatThrownBy(() -> sendSpans(sender, CLIENT_SPAN, CLIENT_SPAN))
      .isInstanceOf(IOException.class);
  }

  @Test void illegalToSendWhenClosed() {
    sender.close();

    assertThatThrownBy(() -> sendSpans(sender, CLIENT_SPAN, CLIENT_SPAN))
      .isInstanceOf(ClosedSenderException.class);
  }

  @Test void endpointSupplierFactory_defaultsToConstant() {
    // The default connection supplier returns a constant URL
    assertThat(sender)
      .extracting("urlSupplier.url")
      .isEqualTo(HttpUrl.parse("http://localhost:19092"));
  }

  @Test void endpointSupplierFactory_constant() {
    sender.close();
    sender = sender.toBuilder()
      .endpointSupplierFactory(e -> new Constant("http://localhost:29092"))
      .build();

    // The connection supplier has a constant URL
    assertThat(sender)
      .extracting("urlSupplier.url")
      .isEqualTo(HttpUrl.parse("http://localhost:29092"));
  }

  @Test void endpointSupplierFactory_constantBad() {
    OkHttpSender.Builder builder = sender.toBuilder()
      .endpointSupplierFactory(e -> new Constant("htp://localhost:9411/api/v1/spans"));

    assertThatThrownBy(builder::build)
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessageContaining("invalid POST url");
  }

  @Test void endpointSupplierFactory_dynamic() {
    AtomicInteger closeCalled = new AtomicInteger();
    HttpEndpointSupplier dynamicEndpointSupplier = new HttpEndpointSupplier() {
      @Override public String get() {
        throw new UnsupportedOperationException();
      }

      @Override public void close() {
        closeCalled.incrementAndGet();
      }
    };

    sender.close();
    sender = sender.toBuilder()
      .endpointSupplierFactory(e -> dynamicEndpointSupplier)
      .build();

    // The connection supplier is deferred until send
    assertThat(sender)
      .extracting("urlSupplier.endpointSupplier")
      .isEqualTo(dynamicEndpointSupplier);

    assertThatThrownBy(() -> sendSpans(sender, CLIENT_SPAN, CLIENT_SPAN))
      .isInstanceOf(UnsupportedOperationException.class);

    // Ensure that closing the sender closes the endpoint supplier
    sender.close();
    sender.close(); // check only closed once
    assertThat(closeCalled).hasValue(1);
  }

  @Test void endpointSupplierFactory_ignoresCloseFailure() {
    AtomicInteger closeCalled = new AtomicInteger();
    HttpEndpointSupplier dynamicEndpointSupplier = new HttpEndpointSupplier() {
      @Override public String get() {
        throw new UnsupportedOperationException();
      }

      @Override public void close() throws IOException {
        closeCalled.incrementAndGet();
        throw new IOException("unexpected");
      }
    };

    sender.close();
    sender = sender.toBuilder()
      .endpointSupplierFactory(e -> dynamicEndpointSupplier)
      .build();

    // Ensure that an exception closing the endpoint supplier doesn't propagate.
    sender.close();
    assertThat(closeCalled).hasValue(1);
  }

  @Test void endpointSupplierFactory_dynamicNull() {
    sender.close();
    sender = sender.toBuilder()
      .endpointSupplierFactory(e -> new BaseHttpEndpointSupplier() {
        @Override public String get() {
          return null;
        }
      })
      .build();

    assertThatThrownBy(() -> sendSpans(sender, CLIENT_SPAN, CLIENT_SPAN))
      .isInstanceOf(NullPointerException.class)
      .hasMessage("endpointSupplier.get() returned null");
  }

  @Test void endpointSupplierFactory_dynamicBad() {
    sender.close();
    sender = sender.toBuilder()
      .endpointSupplierFactory(e -> new BaseHttpEndpointSupplier() {
        @Override public String get() {
          return "htp://localhost:9411/api/v1/spans";
        }
      })
      .build();

    assertThatThrownBy(() -> sendSpans(sender, CLIENT_SPAN, CLIENT_SPAN))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessageContaining("invalid POST url");
  }

  /**
   * The output of toString() on {@link BytesMessageSender} implementations appears in thread names
   * created by {@link AsyncReporter}. Since thread names are likely to be exposed in logs and other
   * monitoring tools, care should be taken to ensure the toString() output is a reasonable length
   * and does not contain sensitive information.
   */
  @Test void toStringContainsOnlySummaryInformation() {
    assertThat(sender).hasToString("OkHttpSender{http://localhost:19092/}");
  }

  @Test void outOfBandCancel() {
    HttpCall call = (HttpCall) sender.sendSpans(Collections.emptyList());
    call.cancel();

    assertThat(call.isCanceled()).isTrue();
  }

  @Test void bugGuardCache() {
    assertThat(sender.client.cache())
      .withFailMessage("senders should not open a disk cache")
      .isNull();
  }

  static void sendSpans(BytesMessageSender sender, Span... spans) throws IOException {
    SpanBytesEncoder bytesEncoder;
    switch (sender.encoding()) {
      case JSON:
        bytesEncoder = SpanBytesEncoder.JSON_V2;
        break;
      case THRIFT:
        bytesEncoder = SpanBytesEncoder.THRIFT;
        break;
      case PROTO3:
        bytesEncoder = SpanBytesEncoder.PROTO3;
        break;
      default:
        throw new UnsupportedOperationException("encoding: " + sender.encoding());
    }
    sender.send(Stream.of(spans).map(bytesEncoder::encode).collect(toList()));
  }

  static abstract class BaseHttpEndpointSupplier implements HttpEndpointSupplier {
    @Override public void close() {
    }
  }
}
