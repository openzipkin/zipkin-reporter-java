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

import com.facebook.swift.codec.ThriftCodecManager;
import com.facebook.swift.codec.ThriftEnumValue;
import com.facebook.swift.codec.ThriftField;
import com.facebook.swift.codec.ThriftStruct;
import com.facebook.swift.service.ThriftMethod;
import com.facebook.swift.service.ThriftServer;
import com.facebook.swift.service.ThriftServerConfig;
import com.facebook.swift.service.ThriftService;
import com.facebook.swift.service.ThriftServiceProcessor;
import java.io.IOException;
import java.util.List;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import zipkin.reporter.libthrift.LibthriftSender;

import static java.util.Collections.emptyList;

public class LibthriftSenderBenchmarks extends SenderBenchmarks {
  ThriftServer scribe;

  @ThriftService("Scribe")
  public static final class Scribe {

    @ThriftMethod(value = "Log")
    public Scribe.ResultCode log(@ThriftField(value = 1) List<Scribe.LogEntry> messages) {
      return ResultCode.OK;
    }

    public enum ResultCode {
      OK(0), TRY_LATER(1);

      final int value;

      ResultCode(int value) {
        this.value = value;
      }

      @ThriftEnumValue
      public int value() {
        return value;
      }
    }

    @ThriftStruct(value = "LogEntry")
    public static final class LogEntry {

      @ThriftField(value = 1)
      public String category;

      @ThriftField(value = 2)
      public String message;
    }
  }

  @Override Sender createSender() throws Exception {
    ThriftServiceProcessor processor =
        new ThriftServiceProcessor(new ThriftCodecManager(), emptyList(), new Scribe());
    scribe = new ThriftServer(processor, new ThriftServerConfig().setPort(9410)).start();
    return LibthriftSender.create("127.0.0.1");
  }

  @Override void afterSenderClose() throws IOException {
    scribe.close();
  }

  // Convenience main entry-point
  public static void main(String[] args) throws RunnerException {
    Options opt = new OptionsBuilder()
        .include(".*" + LibthriftSenderBenchmarks.class.getSimpleName() + ".*")
        .build();

    new Runner(opt).run();
  }
}
