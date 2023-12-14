# zipkin-sender-okhttp3
This module contains a span sender for [OkHttp](https://github.com/square/okhttp) 3.x.

Please view [OkHttpSender](src/main/java/zipkin2/reporter/okhttp3/OkHttpSender.java)
for usage details.

## Compatability

In order to keep new users current, this assigns the most recent major version
of OkHttp 4.x. This reduces distraction around CVEs at the cost of more
dependencies, notably Kotlin, vs the 3.x versions. That said, this module is
tested to be runtime compatible with 3.x, for users who need to support Java 7
or non-Kotlin use cases.
