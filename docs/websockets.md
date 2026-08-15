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

## Limitations

- **Profile-based tests** (`stress_test`, `spike_test`) currently only support
  `handshake` mode. Stream and PingPong modes are only available with
  `load_test` (constant concurrency).
- **No message broadcasting** -- each connection sends one message and closes.
  Full-duplex continuous messaging is planned for a future release.
