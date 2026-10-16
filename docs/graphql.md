# GraphQL Load Testing

strobengine benchmarks GraphQL operations over standard HTTP. A GraphQL query
or mutation document is POSTed as `application/json` and GraphQL-level
`errors` are tracked separately from HTTP transport failures.

Subscriptions are benchmarked over WebSockets using the `graphql-transport-ws`
subprotocol (the modern `graphql-ws` library): point the engine at a `ws://` /
`wss://` URL with a `subscription` document and it negotiates the subprotocol,
sends `connection_init`/`subscribe`, and records time-to-first-message (TTFM)
plus inter-message streaming latencies.

## Supported Features

| Feature | Description |
|---------|-------------|
| Queries & Mutations | Benchmark any query/mutation document over HTTP POST |
| Subscriptions | Stream GraphQL events over `graphql-transport-ws` (WebSocket) |
| Variables | Pass a JSON object of variables via `graphql_variables` |
| Named Operations | Select an operation with `graphql_operation_name` |
| Logical Error Tracking | Count responses whose `errors` array is non-empty |
| Operation Type | Reports `query`, `mutation`, or `subscription` |
| Streaming Latencies | TTFM and inter-message interval percentiles (HDR histograms) |
| Custom Headers | Metadata / auth injection via request headers |
| Chaos Testing | Latency spikes, corrupted payloads, connection drops |

GraphQL requests always use `POST` with `Content-Type: application/json`; the
`method`, `body`, and `form` options are ignored (and combining `body`/`form`
with `graphql_query` is rejected).

## Python API

### Benchmark a Query

```python
from strobengine import StrobEngine, RequestOptions

engine = StrobEngine(
    url="http://localhost:8080/graphql",
    concurrency=10,
    duration=30,
    options=RequestOptions(
        graphql_query="{ hero { name friends { name } } }",
    ),
)
summary = engine.run()

print(summary.graphql.operations_total)  # GraphQL operations observed
print(summary.graphql.error_responses_total)  # responses with a non-empty errors[]
print(summary.graphql.operation_type)  # "query"
```

### Benchmark a Mutation with Variables

```python
options = RequestOptions(
    graphql_query="mutation AddHero($name: String!) { addHero(name: $name) { id } }",
    graphql_variables='{"name": "R2-D2"}',
    graphql_operation_name="AddHero",
)
```

### Benchmark a Subscription

A `ws://` / `wss://` URL plus a `subscription` document runs the
`graphql-transport-ws` protocol. Each iteration consumes one streamed message,
recording its inter-message interval; the first message after `subscribe`
records TTFM.

```python
from strobengine import StrobEngine, RequestOptions

engine = StrobEngine(
    url="ws://localhost:8080/graphql",
    concurrency=10,
    duration=30,
    options=RequestOptions(
        graphql_query="subscription { ticks { at count } }",
        graphql_variables='{"stream": "ticks"}',
    ),
)
summary = engine.run()

print(summary.graphql_ws.operations_total)  # subscriptions started
print(summary.graphql_ws.messages_total)  # `next` frames received
print(summary.graphql_ws.ttfm_p90_ms)  # time-to-first-message P90 (ms)
print(summary.graphql_ws.interval_p99_ms)  # inter-message interval P99 (ms)
```

## CLI

```bash
strobengine load http://localhost:8080/graphql \
  --graphql-query '{ hero { name } }' \
  -c 10 -d 30

strobengine load http://localhost:8080/graphql \
  --graphql-query 'mutation AddHero($name: String!) { addHero(name: $name) { id } }' \
  --graphql-variables '{"name": "R2-D2"}' \
  --graphql-operation-name AddHero \
  -c 10 -d 30

# Subscription: use a ws:// or wss:// URL
strobengine load ws://localhost:8080/graphql \
  --graphql-query 'subscription { ticks }' \
  -c 10 -d 30
```

`--graphql-query` also works with the `stress` and `spike` subcommands.

## Subscription Semantics

The engine offers the `graphql-transport-ws` subprotocol and fails the
connection if the server does not negotiate it. It sends `connection_init`,
awaits `connection_ack`, then `subscribe`. Frames are interpreted as:

- `next` — a streamed event: records the inter-message interval (and TTFM for
  the first event after `subscribe`).
- `error` — a GraphQL error payload: counted toward `errors_total` and the
  run's `total_errors` (the transport status stays `200`).
- `complete` — the stream ended; the next iteration re-subscribes.
- `ping` / `pong` — keepalives; `ping` is answered with `pong`.

A read that exceeds the request timeout, or a dropped connection, is reported
as a transport error (status `0`) and the next iteration reconnects.

## Error Semantics

GraphQL reports logical failures as HTTP 200 with a non-empty `errors` array.
strobengine counts such a response toward `total_errors` (so the error rate
reflects failed operations, not just failed transports) while keeping the
HTTP `status_codes` breakdown truthful. The `graphql` report section exposes:

| Field | Meaning |
|-------|---------|
| `operations_total` | Responses parsed as GraphQL operations |
| `error_responses_total` | Responses with a non-empty `errors` array |
| `errors_total` | Total entries across all `errors` arrays |
| `operation_type` | `query`, `mutation`, or `subscription` (from the document) |

Subscriptions report under a separate `graphql_ws` section:

| Field | Meaning |
|-------|---------|
| `operations_total` | Subscription operations started (`subscribe` sent) |
| `messages_total` | `next` frames received |
| `errors_total` | Entries across all `error` frames |
| `ttfm_mean_ms` / `ttfm_p50_ms` / `ttfm_p90_ms` / `ttfm_p99_ms` | Time-to-first-message stats |
| `interval_mean_ms` / `interval_p50_ms` / `interval_p90_ms` / `interval_p99_ms` | Inter-message interval stats |

## Notes

- Responses that are not GraphQL (non-JSON, or JSON without `data`/`errors`)
  are treated as ordinary HTTP responses and do not add to `operations_total`.
- `operation_type` is derived from the configured document's first operation
  keyword; the anonymous `{ ... }` shorthand is treated as a `query`.
