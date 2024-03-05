/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin2.reporter;

/**
 * Utility for encoding one or more elements of a type into a byte array.
 *
 * @param <S> type of the object to encode
 * @since 3.0
 */
public interface BytesEncoder<S> {
  Encoding encoding();

  int sizeInBytes(S input);

  /** Serializes an object into its binary form. */
  byte[] encode(S input);
}
