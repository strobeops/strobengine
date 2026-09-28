# gRPC Load Testing

strobengine supports gRPC load testing with unary RPC calls, base64-encoded
protobuf payloads, configurable deadlines, and chaos fault injection.

## Supported Features

| Feature | Description |
|---------|-------------|
| Unary RPC | Single request/response calls via `tonic` |
| Base64 Payloads | Protobuf-encoded request bodies via `--grpc-payload` |
| Deadlines | Per-RPC deadline in milliseconds via `--grpc-deadline-ms` |
| Custom Headers | Metadata injection via `--header` flag |
| Chaos Testing | Latency spikes, corrupted payloads, metadata corruption, connection drops |

## Python API

### Basic Unary Call

```python
from strobengine import StrobEngine, RequestOptions

engine = StrobEngine(
    url="grpc://localhost:50051",
    concurrency=10,
    duration=30,
    options=RequestOptions(
        grpc_service="helloworld.Greeter",
        grpc_method="SayHello",
        grpc_payload="CgR0ZXN0",  # base64-encoded protobuf
    ),
)
summary = engine.run()
```

### With Deadline

```python
engine = StrobEngine(
    url="grpc://localhost:50051",
    concurrency=10,
    duration=30,
    options=RequestOptions(
        grpc_service="helloworld.Greeter",
        grpc_method="SayHello",
        grpc_payload="CgR0ZXN0",
        grpc_deadline_ms=5000,  # 5-second deadline
    ),
)
summary = engine.run()
```

### With Custom Headers

```python
engine = StrobEngine(
    url="grpc://localhost:50051",
    concurrency=10,
    duration=30,
    options=RequestOptions(
        grpc_service="helloworld.Greeter",
        grpc_method="SayHello",
        grpc_payload="CgR0ZXN0",
        headers=[("authorization", "Bearer token123")],
    ),
)
summary = engine.run()
```

### With Chaos Testing

```python
engine = StrobEngine(
    url="grpc://localhost:50051",
    concurrency=10,
    duration=30,
    options=RequestOptions(
        grpc_service="helloworld.Greeter",
        grpc_method="SayHello",
        grpc_payload="CgR0ZXN0",
        chaos=True,
    ),
)
summary = engine.run()
```

## CLI Usage

### Basic Unary Call

```bash
strobengine load grpc://localhost:50051 \
  --grpc-service helloworld.Greeter \
  --grpc-method SayHello \
  --grpc-payload CgR0ZXN0 \
  -c 10 -d 30
```

### With Deadline

```bash
strobengine load grpc://localhost:50051 \
  --grpc-service helloworld.Greeter \
  --grpc-method SayHello \
  --grpc-payload CgR0ZXN0 \
  --grpc-deadline-ms 5000 \
  -c 10 -d 30
```

### With Custom Headers

```bash
strobengine load grpc://localhost:50051 \
  --grpc-service helloworld.Greeter \
  --grpc-method SayHello \
  --grpc-payload CgR0ZXN0 \
  --header "authorization:Bearer token123" \
  -c 10 -d 30
```

### With Chaos Testing

```bash
strobengine load grpc://localhost:50051 \
  --grpc-service helloworld.Greeter \
  --grpc-method SayHello \
  --grpc-payload CgR0ZXN0 \
  --chaos \
  -c 10 -d 30
```

### JSON Output

```bash
strobengine load grpc://localhost:50051 \
  --grpc-service helloworld.Greeter \
  --grpc-method SayHello \
  --grpc-payload CgR0ZXN0 \
  --json --no-progress \
  -c 10 -d 30
```

## Protocol Detection

strobengine automatically detects gRPC connections based on the URL scheme:

- `grpc://` -- gRPC (plaintext)
- `grpcs://` -- gRPC (TLS)
- `http://` / `https://` -- HTTP engine
- `ws://` / `wss://` -- WebSocket engine

No explicit protocol flag is needed -- the URL scheme determines the engine.

## Metrics

gRPC load tests produce the same `TestSummary` metrics as HTTP/WebSocket:

| Metric | Description |
|--------|-------------|
| `total_requests` | Total RPC call attempts |
| `total_errors` | Failed connections or RPC calls |
| `average_latency_ms` | Mean round-trip latency |
| `p95_latency_ms` | 95th percentile latency |
| `p99_latency_ms` | 99th percentile latency |
| `min_latency_ms` | Minimum latency |
| `p50_latency_ms` | Median latency |
| `p90_latency_ms` | 90th percentile latency |
| `max_latency_ms` | Maximum latency |
| `total_bytes_received` | Total response bytes |
| `status_codes` | Status code distribution |

### gRPC Status Code Mapping

gRPC status codes are mapped to HTTP equivalents for consistent error tracking:

| gRPC Code | Name | HTTP Equivalent |
|-----------|------|-----------------|
| 0 | OK | 200 |
| 1 | Cancelled | 499 |
| 2 | Unknown | 500 |
| 3 | InvalidArgument | 400 |
| 4 | DeadlineExceeded | 504 |
| 5 | NotFound | 404 |
| 7 | PermissionDenied | 403 |
| 8 | ResourceExhausted | 429 |
| 13 | Internal | 500 |
| 14 | Unavailable | 503 |
| 16 | Unauthenticated | 401 |

Status code `0` indicates a connection-level failure (timeout, unreachable, chaos drop).

### Deep Metrics (multiplexed raw-h2 engine)

Enable a second engine with `--grpc-h2-multiplex` (`RequestOptions(grpc_h2_multiplex=True)`)
to collect HTTP/2 stream-concurrency and flow-control telemetry, emitted in the
`grpc` artifact block (CLI + HTML panels) when nonzero:

| Metric | Description |
|--------|-------------|
| `active_streams_peak` | Peak concurrent in-flight RPCs on the shared HTTP/2 connection |
| `concurrency_utilization_peak` | Peak `active_streams / SETTINGS_MAX_CONCURRENT_STREAMS` |
| `concurrency_utilization_mean` | Mean utilization across samples |
| `window_exhaustion_events_total` | Send stalls where the flow-control window was exhausted (zero credit) |
| `window_stall_duration_ms_total` | Cumulative milliseconds spent blocked awaiting a `WINDOW_UPDATE` |
| `send_capacity_min_bytes` | Minimum effective send-side flow-control credit observed |

The multiplexed engine holds one connection and clones its `SendRequest` per
iteration, so `active_streams` reflects true multiplexing against the peer's
negotiated `SETTINGS_MAX_CONCURRENT_STREAMS`. Sends are driven through
`reserve_capacity`/`poll_capacity`; a stall is recorded once an already-started
body write blocks on zero credit.

**Scope & caveats:** the flag currently supports cleartext `grpc://` only;
`grpcs://` logs a warning and falls back to the default tonic engine.
`send_capacity_min_bytes` is a client-side **proxy** for the connection window
(min effective send credit) — h2 does not expose the raw connection-level window
counter. Metrics are only present when the flag is enabled; the default tonic
path emits no `grpc` block.

## Chaos Testing

gRPC chaos testing applies the same fault injection as HTTP/WebSocket:

| Fault | gRPC Behavior |
|-------|---------------|
| `LatencySpike` | Sleep before connecting |
| `CorruptedPayload` | Send raw corrupted bytes as protobuf payload |
| `MetadataCorruption` | Inject invalid `x-chaos-fault` metadata header |
| `ConnectionDrop` | Timeout connection to 1 nanosecond |

```python
engine = StrobEngine(
    url="grpc://localhost:50051",
    concurrency=10,
    duration=30,
    options=RequestOptions(
        grpc_service="helloworld.Greeter",
        grpc_method="SayHello",
        grpc_payload="CgR0ZXN0",
        chaos=True,
    ),
)
summary = engine.run()
```

## Limitations

- **Unary calls only** -- server-streaming, client-streaming, and bidirectional
  streaming are not yet supported.
- **Profile-based tests** (`stress_test`, `spike_test`) with gRPC URLs are
  not yet fully supported -- gRPC fields may be silently dropped.
