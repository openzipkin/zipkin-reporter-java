package zipkin2.reporter;

/** An exception thrown when a {@link Sender} is used after it has been closed. */
public final class ClosedSenderException extends IllegalStateException {
  static final long serialVersionUID = -4636520624634625689L;
}
