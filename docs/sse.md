# SSE Load Testing

strobengine supports Server-Sent Events (SSE) streaming load testing with
time-to-first-byte (TTFB) tracking, inter-event interval metrics, and
persistent streaming sessions.

## Supported Features

| Feature | Description |
|---------|-------------|
| Stateless Mode | Connect, read one event, close -- measures TTFB |
| Persistent Mode | Maintain a streaming connection across iterations |
| TTFB Tracking | Time from request start to first SSE event |
| Event Interval | Inter-event latency for streaming throughput |
| Max Events | Cap events per connection with `sse_max_events` |
| Custom Headers | Metadata injection via request headers |
| Chaos Testing | Latency spikes, corrupted payloads, connection drops |

## Python API

### Basic SSE Load Test

```python
from strobengine import StrobEngine, RequestOptions

engine = StrobEngine(
    url="http://localhost:8080/sse",
    concurrency=10,
    duration=30,
    options=RequestOptions(sse_enabled=True),
)
summary = engine.run()
```

### With Custom Headers

```python
engine = StrobEngine(
    url="http://localhost:8080/sse",
    concurrency=5,
    duration=30,
    options=RequestOptions(
        sse_enabled=True,
        headers=[("Authorization", "Bearer token123")],
    ),
)
summary = engine.run()
```

### With Max Events

```python
engine = StrobEngine(
    url="http://localhost:8080/sse",
    concurrency=5,
    duration=30,
    options=RequestOptions(
        sse_enabled=True,
        sse_max_events=100,
    ),
)
summary = engine.run()
```

### Using `sse://` URL Scheme

```python
engine = StrobEngine(
    url="sse://localhost:8080/sse",
    concurrency=5,
    duration=30,
)
summary = engine.run()
```

### With Chaos Testing

```python
engine = StrobEngine(
    url="http://localhost:8080/sse",
    concurrency=10,
    duration=30,
    options=RequestOptions(
        sse_enabled=True,
        chaos=True,
    ),
)
summary = engine.run()
```

## CLI Usage

### Basic SSE

```bash
strobengine load http://localhost:8080/sse -c 10 -d 30 --sse
```

### With Max Events

```bash
strobengine load http://localhost:8080/sse -c 10 -d 30 --sse --sse-max-events 100
```

### Stress Test with SSE

```bash
strobengine stress http://localhost:8080/sse --from 5 --to 50 --ramp 30 --hold 60 --sse
```

### Spike Test with SSE

```bash
strobengine spike http://localhost:8080/sse --baseline 5 --peak 100 --spike-duration 10 --sse
```

### With Chaos

```bash
strobengine load http://localhost:8080/sse -c 10 -d 30 --sse --chaos
```

### JSON Output

```bash
strobengine load http://localhost:8080/sse -c 10 -d 30 --sse --json --no-progress
```

## Protocol Detection

strobengine detects SSE connections via URL scheme or explicit flag:

- `sse://` -- SSE over HTTP (auto-normalized to `http://`)
- `sses://` -- SSE over HTTPS (auto-normalized to `https://`)
- `http://` / `https://` with `--sse` flag -- explicit SSE mode

When using `sse://` or `sses://`, no `--sse` flag is needed. The URL is
automatically normalized to `http://` or `https://` for the underlying
connection.

## Metrics

SSE load tests produce the same `TestSummary` metrics as HTTP, plus
aggregated SSE metrics in `TestSummary.sse`:

| Field | Description |
|-------|-------------|
| `total_events_received` | Total SSE events received across all iterations |
| `avg_ttfb_ms` | Average time to first event in milliseconds |

Per-iteration fields available in `RequestMetric`:

| Metric | Description |
|--------|-------------|
| `total_requests` | Total connection attempts |
| `total_errors` | Failed connections or stream errors |
| `average_latency_ms` | Mean iteration latency |
| `p95_latency_ms` | 95th percentile latency |
| `p99_latency_ms` | 99th percentile latency |
| `min_latency_ms` | Minimum latency |
| `p50_latency_ms` | Median latency |
| `p90_latency_ms` | 90th percentile latency |
| `max_latency_ms` | Maximum latency |
| `total_bytes_received` | Total bytes received |
| `status_codes` | Status code distribution (200=success, 0=network error) |
| `sse_events_received` | Number of SSE events received per iteration |
| `sse_first_event_us` | Time to first event in microseconds (TTFB) |
| `sse_event_interval_us` | Average interval between events in microseconds |

## Chaos Testing

SSE chaos testing applies protocol-agnostic faults from the existing
`ChaosEngine`. Enable with `chaos=True`:

| Chaos Fault | SSE Behavior |
|-------------|--------------|
| `LatencySpike` | Sleep before connecting (150ms default) |
| `CorruptedPayload` | No-op (SSE is read-only) |
| `ConnectionDrop` | Skip connection, return error immediately |
| `MetadataCorruption` | No-op (SSE is read-only) |

## How SSE Streaming Works

Each SSE iteration follows this flow:

1. **Connect** -- HTTP GET with `Accept: text/event-stream`
2. **Stream** -- Read bytes from the response stream
3. **Parse** -- Frame events by `\n\n` or `\r\n\r\n` delimiters
4. **Return** -- Yield one `RequestMetric` per event (stateless) or
   per iteration (persistent)

Incomplete frames are buffered across TCP chunk boundaries. The parser
handles split `\r\n\r\n` delimiters gracefully.

## Limitations

- **Binary event data** -- Only text `data:` fields are parsed.
  Binary SSE payloads are not supported.
- **`retry:` field** -- The SSE `retry:` field is parsed but not applied
  to reconnection logic.
- **Event types** -- The `event:` field is captured but does not
  influence routing or filtering.
- **No reconnection** -- If the stream drops, the iteration returns
  an error. Automatic reconnection is not implemented.
