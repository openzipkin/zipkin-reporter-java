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
