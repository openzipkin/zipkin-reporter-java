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
import java.lang.reflect.Constructor;

/** Taken from {@code zipkin2.reporter.internal.Platform} to avoid needing to shade over a single method. */
abstract class Platform {
  private static final Platform PLATFORM = findPlatform();

  Platform() {
  }

  RuntimeException uncheckedIOException(IOException e) {
    return new RuntimeException(e);
  }

  static Platform get() {
    return PLATFORM;
  }

  /** Attempt to match the host runtime to a capable Platform implementation. */
  static Platform findPlatform() {
    // Find JRE 8 new types
    try {
      Class<?> clazz = Class.forName("java.io.UncheckedIOException");
      Constructor<?> ctor = clazz.getConstructor(IOException.class);
      return new Jre8(ctor); // intentionally doesn't access the type prior to the above guard
    } catch (ClassNotFoundException e) {
      // pre JRE 8
    } catch (NoSuchMethodException unexpected) {
      // pre JRE 8
    }
    // compatible with JRE 6
    return Jre6.build();
  }

  static final class Jre8 extends Platform {
    final Constructor<?> uncheckedIOExceptionCtor;

    Jre8(Constructor<?> uncheckedIOExceptionCtor) {
      this.uncheckedIOExceptionCtor = uncheckedIOExceptionCtor;
    }

    @Override public RuntimeException uncheckedIOException(IOException e) {
      try {
        return (RuntimeException) uncheckedIOExceptionCtor.newInstance(e);
      } catch (Exception unexpected) {
        return new RuntimeException(e); // fallback instead of crash
      }
    }
  }

  static final class Jre6 extends Platform {
    static Jre6 build() {
      return new Jre6();
    }
  }
}
