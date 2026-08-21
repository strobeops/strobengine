# HTTP/3 Load Testing

strobengine supports HTTP/3 (QUIC) load testing with TLS 1.3 session resumption,
0-RTT connection benchmarking, and QUIC-specific loss recovery metrics.

## Supported Features

| Feature | Description |
|---------|-------------|
| QUIC Transport | UDP-based transport via Quinn with TLS 1.3 |
| 0-RTT Resumption | Session ticket caching for fast reconnects |
| Loss Recovery | Packet retransmission tracking via QUIC stats |
| Custom Headers | Metadata injection via request headers |
| Request Body | POST/PUT/PATCH with body support |
| Chaos Testing | Latency spikes, corrupted payloads, connection drops |

## Python API

### Basic HTTP/3 Call

```python
from strobengine import StrobEngine, RequestOptions

engine = StrobEngine(
    url="h3://localhost:443/api",
    concurrency=10,
    duration=30,
    options=RequestOptions(
        http3_enabled=True,
    ),
)
summary = engine.run()
```

### With Custom Headers

```python
engine = StrobEngine(
    url="h3://localhost:443/api",
    concurrency=10,
    duration=30,
    options=RequestOptions(
        http3_enabled=True,
        headers=[("Authorization", "Bearer token123")],
    ),
)
summary = engine.run()
```

### With 0-RTT Resumption

```python
engine = StrobEngine(
    url="h3://localhost:443/api",
    concurrency=10,
    duration=30,
    options=RequestOptions(
        http3_enabled=True,
        quic_zero_rtt=True,
    ),
)
summary = engine.run()
```

### With Chaos Testing

```python
engine = StrobEngine(
    url="h3://localhost:443/api",
    concurrency=10,
    duration=30,
    options=RequestOptions(
        http3_enabled=True,
        chaos=True,
    ),
)
summary = engine.run()
```

## CLI Usage

### Basic HTTP/3

```bash
strobengine load h3://localhost:443/api -c 10 -d 30 --http3
```

### With 0-RTT

```bash
strobengine load h3://localhost:443/api -c 10 -d 30 --http3 --quic-zero-rtt
```

### With Custom Idle Timeout

```bash
strobengine load h3://localhost:443/api -c 10 -d 30 --http3 --quic-max-idle-timeout 10000
```

### With Chaos

```bash
strobengine load h3://localhost:443/api -c 10 -d 30 --http3 --chaos
```

### JSON Output

```bash
strobengine load h3://localhost:443/api -c 10 -d 30 --http3 --json --no-progress
```

## Protocol Detection

strobengine automatically detects HTTP/3 connections based on the
URL scheme:

- `h3://` -- HTTP/3 over QUIC
- `http3://` -- HTTP/3 over QUIC (alias)

No explicit protocol flag is needed when using these URL schemes.
The `--http3` flag enables HTTP/3 mode explicitly.

## Metrics

HTTP/3 load tests produce the same `TestSummary` metrics as HTTP, plus
QUIC-specific fields:

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
| `total_bytes_received` | Total bytes received |
| `status_codes` | Status code distribution (200=success, 0=network error) |
| `quic_handshake_us` | QUIC handshake duration (persistent sessions) |
| `quic_0rtt_used` | Whether 0-RTT was accepted (persistent sessions) |
| `quic_retransmits` | Packet retransmission count since last iteration |

## Chaos Testing

HTTP/3 chaos testing applies protocol-agnostic faults from the existing
`ChaosEngine`. Enable with `chaos=True`:

| Chaos Fault | HTTP/3 Behavior |
|-------------|-----------------|
| `LatencySpike` | Sleep before connecting |
| `CorruptedPayload` | Send garbage bytes on H3 stream |
| `ConnectionDrop` | Timeout connection to 1 nanosecond |
| `MetadataCorruption` | No-op (not applicable to HTTP/3) |

## Limitations

- **No HTTP/3 mock server** for success-path E2E tests. Error-path tests
  target unreachable endpoints.
- **0-RTT** requires the target server to issue TLS session tickets.
  Without server support, connections fall back to 1-RTT automatically.
- **Profile-based tests** (`stress_test`, `spike_test`) currently only support
  `handshake` mode for HTTP/3.
