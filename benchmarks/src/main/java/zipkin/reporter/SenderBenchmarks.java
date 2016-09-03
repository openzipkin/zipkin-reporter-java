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

package zipkin.reporter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.AuxCounters;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Group;
import org.openjdk.jmh.annotations.GroupThreads;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import zipkin.Component;
import zipkin.TestObjects;
import zipkin.reporter.internal.AwaitableCallback;

/**
 * These benchmarks measures the throughput of senders by treating them as if they are blocking.
 *
 * <p>By blocking, we can compare performance of each sender somewhat fairly, eventhough in practice
 * many will not block.
 */
@Measurement(iterations = 5, time = 1)
@Warmup(iterations = 10, time = 1)
@Fork(3)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Group)
public abstract class SenderBenchmarks {
  static final byte[] clientSpan = Encoder.THRIFT.encode(TestObjects.TRACE.get(2));

  @AuxCounters
  @State(Scope.Thread)
  public static class SendCounters {
    public int spans;
    public int spansDropped;

    @Setup(Level.Iteration)
    public void clean() {
      spans = spansDropped = 0;
    }
  }

  Sender sender;
  // TODO: is there a way to add another field to JMH output? Ex span size
  List<byte[]> spans;

  @Setup(Level.Trial)
  public void setup() throws Throwable {
    sender = createSender();

    Component.CheckResult senderCheck = sender.check();
    if (!senderCheck.ok) throw new IllegalStateException("sender not ok", senderCheck.exception);

    spans = new ArrayList<>();
    int remaining = sender.messageMaxBytes();
    for (; clientSpan.length < remaining; remaining -= clientSpan.length) {
      spans.add(clientSpan);
    }

    // really, really check everything is ok
    AwaitableCallback callback = new AwaitableCallback();
    sender.sendSpans(spans, callback);
    callback.await();
  }

  abstract Sender createSender() throws Exception;

  @Benchmark @Group("no_contention") @GroupThreads(1)
  public void no_contention_blocking_send(SendCounters counters) {
    sendSpansBlocking(counters);
  }

  @Benchmark @Group("mild_contention") @GroupThreads(2)
  public void mild_contention_blocking_send(SendCounters counters) {
    sendSpansBlocking(counters);
  }

  @Benchmark @Group("high_contention") @GroupThreads(8)
  public void high_contention_blocking_send(SendCounters counters) {
    sendSpansBlocking(counters);
  }

  void sendSpansBlocking(SendCounters counters) {
    AwaitableCallback callback = new AwaitableCallback();
    try {
      sender.sendSpans(spans, callback);
      callback.await();
      counters.spans += spans.size();
    } catch (RuntimeException e) {
      counters.spansDropped += spans.size();
    }
  }

  @TearDown(Level.Trial)
  public void close() throws IOException {
    sender.close();
    afterSenderClose();
  }

  abstract void afterSenderClose() throws IOException;
}

