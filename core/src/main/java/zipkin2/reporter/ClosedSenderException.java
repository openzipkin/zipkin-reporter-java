/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin2.reporter;

/** An exception thrown when a {@link BytesMessageSender} is used after it has been closed. */
public final class ClosedSenderException extends IllegalStateException {
  static final long serialVersionUID = -4636520624634625689L;
}
