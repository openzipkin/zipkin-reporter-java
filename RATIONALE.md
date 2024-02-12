# zipkin-reporter rationale

## HttpEndpointSupplier

`HttpEndpointSupplier` was generalizes endpoint resolution for built-in and third-party senders.
Specifically, it allows Zipkin to be looked up in a way besides DNS.

For example, Netflix/Eureka was formalized in Zipkin 2.27 and until `HttpEndpointSupplier`, there
was no means for Zipkin Reporter users to integrate, except for the discontinued spring-cloud-sleuth
project. By adding support a common hook, any framework, new or old, can integrate a custom
discovery or client-side loadbalancer.

### Why does the `Factory` accept a string endpoint?

Not all discovery solutions include components to build a zipkin endpoint. For example,
Netflix/Eureka instance information includes vipAddress and port details, but not the path, and
which of the secure or non-secure IP addresses should be used. Since HTTP based senders already
configure a static endpoint, this can be re-used to pass a virtual endpoint. Specifically, a fake
hostname can represent a symbolic application, and be substituted with a real host or IP address
later.

A string is used for two reasons. One is that it is the least common denominator across typical
endpoint types, which could be `URL` or a library-specific `HttpUrl`. The other reason is that
allowing the (configuration) endpoint to be a string, a user can supply an invalid URL, but valid
configuration through. For example, it could be a comma-separated list of well-known addresses the
endpoint supplier will seed its configuration with.

### Why does the `HttpEndpointSupplier` only return a single endpoint?

The `HttpEndpointSupplier` returns a single endpoint to integrate with existing
`URLConnectionSender` and `OkHttpSender` logic, which doesn't act like a client-side loadbalancer.
While HTTP libraries may internally resolve a name to multiple IP addresses, this is usually an
internal detail.

Someone who wants to employ client-side loadbalancer logic can do that inside the
`HttpEndpointSupplier`, and return a chosen endpoint. This will work because the supplier is
documented to not be cached, unless it is a `ConstantHttpEndpointSupplier`.

### Why is `HttpEndpointSupplier` closeable?

Just like `BytesMessageSender`, an `HttpEndpointSupplier` can open resources. An example could be
an HTTP client for the Netflix Eureka API. A sender is passed an endpoint supplier factory and uses
it during construction, usually in `senderBuilder.build()`. The caller of `senderBuilder.build()`
has no reference to the `HttpEndpointSupplier` created. Hence, the sender needs to close it, during
`sender.close()`.

## Sending an empty list is permitted

Historically, we had a `Sender.check()` function for fail fast reasons, but it was rarely used and
rarely implemented correctly. In some cases, people returned `OK` having no knowledge of if the
health was good or not. In one case, Stackdriver, a seemingly good implementation was avoided for
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
Stackdriver did. For example, verifying broker connectivity doesn't mean the queue or topic works.
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

The main driving points were how niche this function is (not called by many, or on interval),
and how much work it is to implement a `check()` vs allowing an empty send to proceed. In the
current code base, the only work required for the latter was documentation, as all senders would
pass an empty list. Secondary driving force was that the `BytesMessageSender` main goal is easier
implementation and re-introducing a bad `check()` api gets in the way of this.

Due to the complexity of this problem, we decided that rather to leave empty undefined, document
sending empty is ok. This allows a couple users to implement a fail-fast in a portable way, without
burdening implementers of `BytesMessageSender` with an unimplementable or wrong `check()` function
for most platforms.
