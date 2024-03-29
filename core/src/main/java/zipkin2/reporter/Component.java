/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
package zipkin2.reporter;

import java.io.Closeable;
import java.io.IOException;

/**
 * Components are object graphs used to compose a zipkin service or client. For example, a storage
 * component might return a query api.
 *
 * <p>Components are lazy in regard to I/O. They can be injected directly to other components, to
 * avoid crashing the application graph if a network service is unavailable.
 *
 * @since 3.0
 * @deprecated since 3.2 this is no longer used. This will be removed in v4.0.
 */
@Deprecated
public abstract class Component implements Closeable {

  /**
   * Answers the question: Are operations on this component likely to succeed?
   *
   * <p>Implementations should initialize the component if necessary. It should test a remote
   * connection, or consult a trusted source to derive the result. They should use least resources
   * possible to establish a meaningful result, and be safe to call many times, even concurrently.
   *
   * @see CheckResult#OK
   * @deprecated since 3.2 this is no longer used. If you need to check a sender, send a zero-length
   * list of spans. This will be removed in v4.0.
   */
  @Deprecated
  public CheckResult check() {
    return CheckResult.OK;
  }

  /**
   * Closes any network resources created implicitly by the component.
   *
   * <p>For example, if this created a connection, it would close it. If it was provided one, this
   * would close any sessions, but leave the connection open.
   */
  @Override public void close() throws IOException {
  }
}
