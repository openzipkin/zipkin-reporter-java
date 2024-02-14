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
package zipkin2.reporter;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.logging.Logger;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import zipkin2.Span;
import zipkin2.codec.SpanBytesEncoder;

import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static zipkin2.TestObjects.CLIENT_SPAN;
import static zipkin2.reporter.HttpEndpointSuppliers.newConstant;

@ExtendWith(MockitoExtension.class)
class BaseHttpSenderTest {

  @Mock Logger logger;
  @Mock Consumer<List<Span>> onSpans;
  FakeHttpSender sender;

  @BeforeEach
  void newSender() {
    sender = new FakeHttpSender(logger, "http://localhost:19092", onSpans);
  }

  @Test void illegalToSendWhenClosed() {
    sender.close();

    assertThatThrownBy(() -> sendSpans(sender, CLIENT_SPAN, CLIENT_SPAN))
      .isInstanceOf(ClosedSenderException.class);
  }

  @Test void endpointSupplierFactory_defaultsToConstant() {
    // The default connection supplier returns a constant URL
    assertThat(sender.endpoint)
      .isEqualTo("http://localhost:19092");
  }

  @Test void endpointSupplierFactory_constant() {
    sender.close();
    sender = sender.withHttpEndpointSupplierFactory(
      e -> newConstant("http://localhost:29092")
    );

    // The connection supplier has a constant URL
    assertThat(sender.endpoint)
      .isEqualTo("http://localhost:29092");
  }

  @Test void endpointSupplierFactory_constantBad() {
    HttpEndpointSupplier.Factory badFactory =
      e -> newConstant("htp://localhost:9411/api/v1/spans");

    assertThatThrownBy(() -> sender.withHttpEndpointSupplierFactory(badFactory))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessageContaining("unknown protocol: htp");
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
    sender = sender.withHttpEndpointSupplierFactory(e -> dynamicEndpointSupplier);

    // The connection supplier is deferred until send
    assertThat(sender.endpointSupplier)
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
    sender = sender.withHttpEndpointSupplierFactory(e -> dynamicEndpointSupplier);

    // Ensure that an exception closing the endpoint supplier doesn't propagate.
    sender.close();
    assertThat(closeCalled).hasValue(1);
  }

  @Test void endpointSupplierFactory_dynamicNull() {
    sender.close();
    sender = sender.withHttpEndpointSupplierFactory(e -> new BaseHttpEndpointSupplier() {
      @Override public String get() {
        return null;
      }
    });

    assertThatThrownBy(() -> sendSpans(sender, CLIENT_SPAN, CLIENT_SPAN))
      .isInstanceOf(NullPointerException.class)
      .hasMessage("endpointSupplier.get() returned null");
  }

  @Test void endpointSupplierFactory_dynamicBad() {
    sender.close();
    sender = sender.withHttpEndpointSupplierFactory(e -> new BaseHttpEndpointSupplier() {
      @Override public String get() {
        return "htp://localhost:9411/api/v1/spans";
      }
    });

    assertThatThrownBy(() -> sendSpans(sender, CLIENT_SPAN, CLIENT_SPAN))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessageContaining("unknown protocol: htp");
  }

  /**
   * The output of toString() on {@link BytesMessageSender} implementations appears in thread names
   * created by {@link AsyncReporter}. Since thread names are likely to be exposed in logs and other
   * monitoring tools, care should be taken to ensure the toString() output is a reasonable length
   * and does not contain sensitive information.
   */
  @Test void toStringContainsOnlySummaryInformation() {
    assertThat(sender).hasToString("FakeHttpSender{http://localhost:19092}");
  }

  static void sendSpans(BytesMessageSender sender, Span... spans) throws IOException {
    sender.send(Stream.of(spans).map(SpanBytesEncoder.JSON_V2::encode).collect(toList()));
  }

  static abstract class BaseHttpEndpointSupplier implements HttpEndpointSupplier {
    @Override public void close() {
    }
  }
}
