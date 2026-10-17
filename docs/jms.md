# JMS / Enterprise Messaging Load Testing

strobengine benchmarks enterprise message brokers — the systems behind **JMS**
(Java Message Service) — by speaking **STOMP 1.2** (Simple/Streaming Text
Oriented Messaging Protocol). JMS is a Java API rather than a wire protocol, so
there is no JMS wire format to speak directly; STOMP is the common denominator
exposed by the mainstream JMS brokers (Apache ActiveMQ "Classic" and ActiveMQ
Artemis), which makes it the right transport for queue throughput and delivery
latency benchmarking.

Point a `stomp://` URL at a broker and strobengine either publishes messages
(**produce**) or subscribes and drains them (**consume**), reporting broker
throughput, send/ack latency, time-to-first-message, and inter-message
intervals.

## Supported Features

| Feature | Description |
|---------|-------------|
| STOMP 1.2 | Text frames over TCP (`stomp://host:61613`) |
| Produce mode | One `SEND` per iteration to a destination (`--stomp-mode produce`) |
| Consume mode | `SUBSCRIBE` + one `MESSAGE` drained per iteration (`--stomp-mode consume`) |
| Receipts | Opt-in broker acknowledgement per `SEND` for true ack latency (`--stomp-receipt`) |
| Ack modes | `auto`, `client`, `client-individual` (`--stomp-ack`) |
| Custom Headers | Arbitrary STOMP headers via `--stomp-header key:value` |
| Persistent sessions | One connection per worker, reused across iterations |
| Chaos Testing | Latency spikes, corrupted payloads, metadata corruption, connection drops |

STOMP heart-beating is negotiated **off** (`heart-beat:0,0`) for this first
release: under sustained load a heartbeat policy risks false disconnects and
skews throughput. The client still tolerates inbound heartbeat newlines.

## Python API

Produce (publish throughput):

```python
from strobengine import StrobEngine, RequestOptions
from strobengine.engine import StompModeEnum

engine = StrobEngine(
    url="stomp://localhost:61613",
    concurrency=10,
    duration=30,
    options=RequestOptions(
        stomp_destination="/queue/orders",
        stomp_message='{"order_id": 1}',
        stomp_mode=StompModeEnum.produce,
    ),
)
summary = engine.run()

print(summary.stomp.messages_sent_total)  # messages published
print(summary.stomp.send_latency_p50_ms)  # broker-ack latency when receipts are on
```

Consume (delivery throughput):

```python
from strobengine.engine import StompAckEnum, StompModeEnum

engine = StrobEngine(
    url="stomp://localhost:61613",
    concurrency=4,
    duration=30,
    options=RequestOptions(
        stomp_destination="/queue/orders",
        stomp_mode=StompModeEnum.consume,
        stomp_ack=StompAckEnum.auto,
    ),
)
summary = engine.run()

print(summary.stomp.messages_received_total)  # messages delivered
print(summary.stomp.ttfm_p50_ms)  # time from SUBSCRIBE to first message
print(summary.stomp.receive_interval_p50_ms)  # inter-message gap
```

## CLI

```bash
# Publish to a queue
strobengine load stomp://localhost:61613 \
  --stomp-destination /queue/orders \
  --stomp-message '{"order_id": 1}' \
  --stomp-mode produce \
  -c 10 -d 30

# Consume from a queue
strobengine load stomp://localhost:61613 \
  --stomp-destination /queue/orders \
  --stomp-mode consume \
  -c 4 -d 30
```

The STOMP options also work with the `stress` and `spike` subcommands.

## Produce vs Consume

| Mode | Iteration | Primary metrics |
|------|-----------|-----------------|
| `produce` | one `SEND` to the destination | `messages_sent_total`, `send_latency_*_ms` |
| `consume` | one `MESSAGE` drained after a single `SUBSCRIBE` | `messages_received_total`, `ttfm_*_ms`, `receive_interval_*_ms` |

### Receipts (fire-and-forget vs acknowledged)

By default `SEND` is **fire-and-forget** (no `receipt` header), which measures
pure publish throughput. Passing `--stomp-receipt` adds a `receipt` header and
blocks on the broker's `RECEIPT` frame, so `send_latency_*_ms` becomes a true
**broker-ack latency**. Receipts are incompatible with `consume` mode (rejected
at configuration time). The `stomp` report section records `receipts_enabled`
so the two latency semantics are never conflated.

## Metrics

The `stomp` report section exposes:

| Field | Meaning |
|-------|---------|
| `messages_sent_total` | `SEND` frames accepted (produce) |
| `messages_received_total` | `MESSAGE` frames drained (consume) |
| `bytes_sent_total` | Message bytes published |
| `errors_total` | Protocol errors, broker `ERROR` frames, or connection failures |
| `reconnects_total` | Sessions re-established after a failure |
| `destination` | Queue/topic destination (from `--stomp-destination`) |
| `mode` | `produce` or `consume` |
| `ack_mode` | `auto`, `client`, or `client-individual` |
| `receipts_enabled` | Whether `send_latency_*` is a broker-ack latency |
| `send_latency_*_ms` | Mean/p50/p90/p99 publish latency |
| `receive_interval_*_ms` | Mean/p50/p90/p99 inter-message gap |
| `ttfm_*_ms` | Mean/p50/p90/p99 time-to-first-message after `SUBSCRIBE` |

## Notes and Limitations

- **TLS is not yet supported**: `stomps://` is rejected at configuration time
  rather than silently downgraded to cleartext. Use `stomp://` on trusted
  networks.
- **Heart-beating is disabled** in this release (see above).
- A `stomp://` URL requires `--stomp-destination`; the destination defaults are
  never guessed.
- The default TCP port is `61613` (the IANA-registered STOMP port); override it
  in the URL (`stomp://host:port`).
- Connections are persistent per worker; a failed session is reset and
  re-established on the next iteration, and the reconnect is counted in
  `reconnects_total`.
