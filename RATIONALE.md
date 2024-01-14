# zipkin-reporter rationale

## Sending an empty list is permitted

Historically, we had a `Sender.check()` function for fail fast reasons, but it was rarely used and
rarely implemented correctly. In some cases, people returned `OK` having no knowledge of if the
health was good or not. In one case, stackdriver, a seemingly good implementation was avoided for
directly sending an empty list of spans, until `check()` was changed to do the same. Rather than
define a poorly implementable `Sender.check()` which would likely still require sending an empty
list, we decided to document a call to send no spans should pass through.

Two known examples of using `check()` were in server modules that forward spans with zipkin reporter
and finagle. `zipkin-finagle` is no longer maintained, so we'll focus on the server modules.

zipkin-stackdriver (now zipkin-gcp) was both important to verify and difficult to implement a
meaningful `check()`. First attempts looked good, but would pass even when users had no permission
to write spans. For this reason, people ignored the check and did out-of-band sending zero spans to
the POST endpoint. Later, this logic was made the primary impl of `check()`.

In HTTP senders a `check()` would be invalid for non-intuitive reasons unless you also just posted
no spans. For example, while zipkin has a `/health` endpoint, most clones do not implement that or
put it at a different path. So, you can't check with `/health` and are left with either falsely
returning `OK` or sending an empty list of spans.

Note that zipkin server does obviate calls to storage when incoming lists are empty. This is not
just for things like this, but 3rd party instrumentation which bugged out and sent no spans.

Messaging senders came close to implementing health except would suffer similar problems as
StackDriver did. For example, verifying broker connectivity doesn't mean the queue or topic works.
While you can dig around and solve this for some brokers, it ends up the same situation.

Another way could be to catch an exception from a prior "POST", and if that failed, return a
corresponding status. This could not be for fail-fast because the caller wouldn't have any spans to
send, yet. It is complicated code for a function uncommon in instrumentation and the impl would be
hard to reason with concurrently.

The main problem is that we used the same `Component` type in reporter as we did for zipkin server,
which defined `check()` in a hardly used and hardly implementable way except sending no spans.

We had the following choices:

* force implementation of `check()` knowing its problems and that it is usual in instrumentation
* document that implementors can skip `send(empty)` even though call sites use this today
* document that you should not skip `send(empty)`, so that the few callers can use it for fail-fast

The main driving points were how niche this function is (not called by many, and often on interval),
and how much work it is to implement a `check()` vs allowing an empty send to proceed. In the
current code base, the only work required for the latter was documentation, as all senders would
pass an empty list. Secondary driving force was that the `BytesMessageSender` main goal is easier
implementation and re-introducing a bad `check()` api gets in the way of this.

Due to the complexity of this problem, we decided that rather to leave empty undefined, document
sending empty is ok. This allows a couple users to implement a fail-fast in a portable way, without
burdening implementers of `BytesMessageSender` with an unimplementable or wrong `check()` function
for most platforms.
