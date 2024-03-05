/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin2.reporter.internal;

import java.io.IOException;
import java.lang.reflect.Constructor;

/** Taken from {@code zipkin2.reporter.internal.Platform} to avoid needing to shade over a single method. */
public abstract class Platform {
  private static final Platform PLATFORM = findPlatform();

  Platform() {
  }

  public RuntimeException uncheckedIOException(IOException e) {
    return new RuntimeException(e);
  }

  public static Platform get() {
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
