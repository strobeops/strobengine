# WebSocket Load Testing

strobengine supports WebSocket load testing with configurable execution
modes for handshake, ping-pong, and message streaming scenarios.

## Supported Modes

| Mode | Description |
|------|-------------|
| `handshake` | Connect and immediately close (default) |
| `ping_pong` | Send Ping frame, wait for Pong, then close |
| `stream` | Send a text payload, await response frame, then close |

## Python API

### Handshake Mode (Default)

```python
from strobengine import StrobEngine

engine = StrobEngine(
    url="ws://localhost:8080/ws",
    concurrency=10,
    duration=30,
)
summary = engine.run()
```

### PingPong Mode

```python
from strobengine import StrobEngine, RequestOptions

engine = StrobEngine(
    url="ws://localhost:8080/ws",
    concurrency=10,
    duration=30,
    options=RequestOptions(ws_mode="ping_pong"),
)
summary = engine.run()
```

### Stream Mode with Custom Payload

```python
from strobengine import StrobEngine, RequestOptions

engine = StrobEngine(
    url="ws://localhost:8080/ws",
    concurrency=10,
    duration=30,
    options=RequestOptions(
        ws_mode="stream",
        ws_payload='{"type": "echo", "data": "test"}',
    ),
)
summary = engine.run()
```

### Stream Mode with Default Payload

When `ws_payload` is not provided, the engine defaults to sending `"ping"`.

```python
from strobengine import StrobEngine, RequestOptions

engine = StrobEngine(
    url="ws://localhost:8080/ws",
    concurrency=10,
    duration=30,
    options=RequestOptions(ws_mode="stream"),
)
summary = engine.run()
```

## Custom Headers

```python
from strobengine import StrobEngine, RequestOptions

engine = StrobEngine(
    url="ws://localhost:8080/ws",
    concurrency=10,
    duration=30,
    options=RequestOptions(
        headers=[("Authorization", "Bearer token123")],
    ),
)
summary = engine.run()
```

## CLI Usage

### Handshake Mode (Default)

```bash
strobengine load ws://localhost:8080/ws -c 10 -d 30
```

### Stream Mode with Custom Payload

```bash
strobengine load ws://localhost:8080/ws -c 10 -d 30 \
  --ws-mode stream \
  --ws-payload '{"type": "echo", "data": "test"}'
```

### PingPong Mode

```bash
strobengine load ws://localhost:8080/ws -c 10 -d 30 --ws-mode ping_pong
```

### Custom Headers

```bash
strobengine load ws://localhost:8080/ws -c 10 -d 30 \
  --header "Authorization: Bearer token123"
```

### Chaos Testing

```bash
strobengine load ws://localhost:8080/ws -c 10 -d 30 \
  --ws-mode stream --ws-payload "hello" --chaos
```

### JSON Output

```bash
strobengine load ws://localhost:8080/ws -c 10 -d 30 --json --no-progress
```

## Protocol Detection

strobengine automatically detects WebSocket connections based on the
URL scheme:

- `ws://` -- WebSocket engine (plaintext)
- `wss://` -- WebSocket engine (TLS)
- `http://` / `https://` -- HTTP engine

No explicit protocol flag is needed -- the URL scheme determines the engine.

## Metrics

WebSocket load tests produce the same `TestSummary` metrics as HTTP:

| Metric | Description |
|--------|-------------|
| `total_requests` | Total connection attempts |
| `total_errors` | Failed handshakes or connections |
| `average_latency_ms` | Mean connection/round-trip latency |
| `p95_latency_ms` | 95th percentile latency |
| `p99_latency_ms` | 99th percentile latency |
| `min_latency_ms` | Minimum latency |
| `p50_latency_ms` | Median latency |
| `p90_latency_ms` | 90th percentile latency |
| `max_latency_ms` | Maximum latency |
| `total_bytes_received` | Total bytes received (Stream mode) |
| `avg_e2e_latency_us` | Average cross-client broadcast latency (Pub/Sub mode) |
| `status_codes` | Status code distribution (200=success, 0=network error) |

## Chaos Testing

WebSocket chaos testing applies protocol-agnostic faults from the existing
`ChaosEngine`. Enable with `chaos=True`:

```python
from strobengine import StrobEngine, RequestOptions

engine = StrobEngine(
    url="ws://localhost:8080/ws",
    concurrency=10,
    duration=30,
    options=RequestOptions(
        ws_mode="stream",
        ws_payload="hello",
        chaos=True,
    ),
)
summary = engine.run()
```

### Fault Mapping

| Chaos Fault | WebSocket Behavior |
|-------------|-------------------|
| `LatencySpike` | Sleep before connecting (configurable duration) |
| `CorruptedPayload` | Send binary frame with raw corrupted bytes (`\xff\xfe\xbd\xef`) |
| `ConnectionDrop` | Timeout connection to 1 nanosecond (immediate failure) |
| `MetadataCorruption` | No-op (not applicable to WebSocket protocol) |

Chaos faults are applied per-iteration, so a percentage of connections
will experience faults while others proceed normally (controlled by
`chaos_rate`, default 10%).

### Metrics Under Chaos

When chaos is enabled, expect:

- `total_errors > 0` for ConnectionDrop and CorruptedPayload faults
- `status_codes` will show `0` for network-level failures
- The load testing engine itself should never crash -- all faults are
  caught and reported in metrics

## Pub/Sub Mode

strobengine supports publisher/subscriber (fan-out) topologies for
broadcasting scenarios. Workers are assigned roles as either publishers
or subscribers, with cross-client latency measurement via embedded
nanosecond timestamps.

### Supported Roles

| Role | Behavior |
|------|----------|
| `publisher` | Connects once (persistent), sends timestamped binary frames each iteration |
| `subscriber` | Connects once (persistent), passively receives broadcast frames each iteration |

### Configuration Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `ws_role` | `str \| None` | `None` | Worker role: `"publisher"` or `"subscriber"` |
| `ws_publish_interval_ms` | `int \| None` | `None` | Interval between publisher sends (ms) |
| `ws_subscribers` | `int \| None` | `None` | Number of subscriber workers (informational) |

### Python API — Publisher

```python
from strobengine import StrobEngine, RequestOptions

engine = StrobEngine(
    url="ws://localhost:8080/ws/broadcast",
    concurrency=10,
    duration=30,
    options=RequestOptions(
        ws_role="publisher",
        ws_publish_interval_ms=100,
    ),
)
summary = engine.run()
```

### Python API — Subscriber

```python
from strobengine import StrobEngine, RequestOptions

engine = StrobEngine(
    url="ws://localhost:8080/ws/broadcast",
    concurrency=10,
    duration=30,
    options=RequestOptions(
        ws_role="subscriber",
        ws_subscribers=10,
    ),
)
summary = engine.run()
```

### CLI — Publisher

```bash
strobengine load ws://localhost:8080/ws/broadcast -c 10 -d 30 \
  --ws-role publisher \
  --ws-publish-interval 100
```

### CLI — Subscriber

```bash
strobengine load ws://localhost:8080/ws/broadcast -c 10 -d 30 \
  --ws-role subscriber \
  --ws-subscribers 10
```

### Cross-Client Latency Measurement

Publishers embed a 16-byte big-endian nanosecond timestamp (UNIX epoch)
at the head of each binary frame. Subscribers parse this timestamp on
receive and compute end-to-end broadcast latency:

```
e2e_latency_us = (receive_time_ns - sent_time_ns) / 1000
```

This metric is available in `TestSummary.avg_e2e_latency_us` and is
displayed in the reporter output when non-zero.

### Metrics for Pub/Sub

| Metric | Description |
|--------|-------------|
| `avg_e2e_latency_us` | Average cross-client broadcast latency (microseconds) |
| `total_bytes_received` | Total bytes received by subscribers |
| `total_requests` | Total iterations across all workers |

## Limitations

- **Profile-based tests** (`stress_test`, `spike_test`) currently only support
  `handshake` mode. Stream, PingPong, and Pub/Sub modes are only available with
  `load_test` (constant concurrency).
