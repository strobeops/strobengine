# Distributed Load Testing

strobengine can distribute a load test across multiple machines: a single
**Master controller** coordinates many **Worker daemons** that generate traffic
in parallel. Workers register with the master, report health via heartbeats,
receive a synchronized start command, and stream rolling telemetry back for
live dashboards and a unified final report.

This is separate from the single-process Python API and CLI. It uses two Rust
binaries — `strobengine-master` and `strobengine-worker` — that are built with
Cargo and are **not** part of the `pip` wheel.

## Architecture

```
                    control plane (gRPC over HTTP/2)
  ┌───────────────┐   RegisterWorker / Heartbeat / WatchCommands
  │    Master     │◀──────────────────────────────────────────────┐
  │  controller   │   StreamMetrics  (client streaming, MetricBatch)│
  └───────┬───────┘                                              │
          │  barrier → synchronized START (CommandRequest)        │
          ▼                                                      │
  ┌──────────┐   ┌──────────┐   ┌──────────┐                     │
  │ Worker 1 │   │ Worker 2 │   │ Worker 3 │  …                  │
  └────┬─────┘   └────┬─────┘   └────┬─────┘                     │
       └──────────────┼──────────────┘   HTTP load                │
                      ▼                                            │
                 ┌─────────┐
                 │ Target  │
                 └─────────┘
```

Two gRPC services are defined in `proto/loadtesting/v1/load_test_controller.proto`:

| Service | RPC | Direction | Purpose |
|---------|-----|-----------|---------|
| `MasterController` | `RegisterWorker` | worker → master | Register capabilities and receive cluster config |
| `MasterController` | `Heartbeat` | worker → master | Liveness, state, active VUs, resource usage |
| `MasterController` | `WatchCommands` | master → worker (stream) | Push `START`/`STOP`/`ABORT` lifecycle commands |
| `MasterController` | `AcknowledgeCommand` | worker → master | Acknowledge a received command |
| `TelemetryService` | `StreamMetrics` | worker → master (stream) | Stream rolling `MetricBatch` windows |

## Prerequisites & building

- A Rust toolchain (see `docs/install.md` for the pinned toolchain).
- Build the two binaries from the repository root:

```bash
cargo build --release --bin strobengine-master --bin strobengine-worker
# binaries land in target/release/
```

Both daemons log through `tracing`; set `RUST_LOG=info` (or `debug`, `trace`)
to control verbosity.

## Quick start (single machine)

This runs a master and two workers locally against one target.

```bash
# 1. Terminal A — master: serve and drive a single 30s run against the target.
strobengine-master \
  --bind 127.0.0.1 --port 50051 \
  --expected-workers 2 \
  --run-url http://127.0.0.1:8080/health \
  --run-vus 20 \
  --run-duration-secs 30 \
  --report report.json

# 2. Terminal B and C — start two workers pointing at the master.
strobengine-worker --master-endpoint http://127.0.0.1:50051 --worker-id worker-1
strobengine-worker --master-endpoint http://127.0.0.1:50051 --worker-id worker-2
```

When both workers are registered and report `READY`, the master's barrier is
satisfied, a synchronized `START` is broadcast, and the live dashboard shows
progress:

```
⠁ [00:00:12] [=================>--------------] 12s/30s | 4210 rps | 0.00% err | p50 2.1ms p95 6.4ms p99 11.8ms | 2 workers
```

When every worker's telemetry stream closes, the run finalizes, a summary is
printed, and the aggregated JSON report is written to `report.json`.

> The master only runs a test when a run spec is supplied (`--run-url`,
> `--run-vus`, `--run-duration-secs`). Without it, the master serves the control
> plane and waits — useful when you drive runs programmatically (see
> [Rust subscriber hook](#live-dashboard--rust-subscriber-hook)).

## Cluster deployment

Run one master on a coordinator node and a worker on each load-generator node.
Point every worker at the master's reachable address.

```bash
# Coordinator
strobengine-master --bind 0.0.0.0 --port 50051 --cluster-id prod --expected-workers 8

# Each load generator (worker id defaults to the hostname)
strobengine-worker \
  --master-endpoint http://10.0.0.5:50051 \
  --max-vus 2000 \
  --label zone=eu-west,pool=web
```

Notes:

- The master's `--heartbeat-interval-ms` is advertised to workers and **takes
  precedence** over a worker's own heartbeat interval.
- `--worker-timeout-ms 0` (the default) evicts a worker after three missed
  heartbeats; set an explicit value if your network is slow.
- `--tls` requires workers to connect over TLS; provide an `https://` master
  endpoint to the workers. (In this release the flag is advisory — the scheme
  of `--master-endpoint` determines the transport.)
- Open the master port (default `50051`) to all worker nodes; workers dial out,
  so they need no inbound ports.

## Run lifecycle

1. **Register** — each worker calls `RegisterWorker` with its id, hostname, IP,
   VU ceiling, and hardware capacity; the master replies with cluster config.
2. **Heartbeat & health** — workers send periodic heartbeats (state, active VUs,
   resource usage). A background sweep marks a worker `DISCONNECTED` after the
   worker timeout and drops its command stream.
3. **Barrier** — the master waits until the expected number of workers are
   `READY` **and** have an open command stream, up to `--barrier-timeout-ms`.
   `--expected-workers 0` allocates every currently-ready worker.
4. **Synchronized START** — a single `START` (carrying the `TestConfiguration`)
   is broadcast to all allocated workers to eliminate ramp-up skew.
5. **Execution** — each worker runs its virtual users against the target and
   buffers request outcomes into fixed time windows.
6. **Telemetry** — workers stream `MetricBatch` windows (one per second by
   default) via `StreamMetrics`; the master merges them in real time.
7. **Finalize** — when every worker's telemetry stream closes (or a
   duration-derived deadline passes), the master produces the final report.

## Master CLI reference

| Flag | Env | Default | Description |
|------|-----|---------|-------------|
| `--cluster-id` | `STROBENGINE_CLUSTER_ID` | `strobengine` | Cluster id handed to every worker |
| `--bind` | `STROBENGINE_BIND` | `0.0.0.0` | Address to bind the gRPC server to |
| `--port` | `STROBENGINE_PORT` | `50051` | Port to bind the gRPC server to |
| `--heartbeat-interval-ms` | `STROBENGINE_HEARTBEAT_INTERVAL_MS` | `2000` | Heartbeat cadence advertised to workers |
| `--metric-flush-interval-ms` | `STROBENGINE_METRIC_FLUSH_INTERVAL_MS` | `1000` | Telemetry flush interval advertised to workers (advisory) |
| `--worker-timeout-ms` | `STROBENGINE_WORKER_TIMEOUT_MS` | `0` | Evict a worker after this long without a heartbeat (`0` = 3× heartbeat) |
| `--sweep-interval-ms` | `STROBENGINE_SWEEP_INTERVAL_MS` | `1000` | Health-sweep cadence |
| `--expected-workers` | `STROBENGINE_EXPECTED_WORKERS` | `0` | Workers to wait for before starting (`0` = all ready) |
| `--barrier-timeout-ms` | `STROBENGINE_BARRIER_TIMEOUT_MS` | `30000` | Barrier timeout |
| `--histogram-bucket-count` | `STROBENGINE_HISTOGRAM_BUCKET_COUNT` | `10` | Latency histogram buckets (must match workers) |
| `--window-secs` | `STROBENGINE_WINDOW_SECS` | `5` | Live sliding-window length |
| `--no-live` | `STROBENGINE_NO_LIVE` | off | Disable the live terminal dashboard |
| `--tls` | `STROBENGINE_TLS` | off | Require TLS on the worker channel |
| `--run-url` | `STROBENGINE_RUN_URL` | none | Target URL for the optional single run |
| `--run-vus` | `STROBENGINE_RUN_VUS` | `0` | Virtual users per worker for the single run |
| `--run-duration-secs` | `STROBENGINE_RUN_DURATION_SECS` | `0` | Duration of the single run, in seconds |
| `--report` | `STROBENGINE_REPORT` | none | Report path (`-` for stdout) |

## Worker CLI reference

| Flag | Env | Default | Description |
|------|-----|---------|-------------|
| `--master-endpoint` | `STROBENGINE_MASTER_ENDPOINT` | *(required)* | Master gRPC endpoint, e.g. `http://127.0.0.1:50051` |
| `--worker-id` | `STROBENGINE_WORKER_ID` | hostname | Stable, cluster-unique worker id |
| `--hostname` | `STROBENGINE_HOSTNAME` | system host name | Reported host name |
| `--ip-address` | `STROBENGINE_IP_ADDRESS` | `127.0.0.1` | Reachable IP advertised to the master |
| `--max-vus` | `STROBENGINE_MAX_VUS` | `0` | VU ceiling (`0` = CPU cores × 100) |
| `--label` | `STROBENGINE_LABELS` | none | Operator tags `key=value` (repeatable or comma-separated) |
| `--heartbeat-interval-ms` | `STROBENGINE_HEARTBEAT_INTERVAL_MS` | `2000` | Heartbeat cadence (overridden by the master's value) |
| `--metric-window-ms` | `STROBENGINE_METRIC_WINDOW_MS` | `1000` | Telemetry aggregation window |
| `--request-timeout-ms` | `STROBENGINE_REQUEST_TIMEOUT_MS` | `10000` | Per-request timeout |
| `--tls` | `STROBENGINE_TLS` | off | Connect to the master over TLS |

## Live dashboard & Rust subscriber hook

While a run is active the master CLI renders a live progress bar (elapsed,
cluster RPS, error rate, p50/p95/p99 latency, active workers), driven by a
broadcast channel of `LiveSnapshot`s. It is enabled by default on a TTY and can
be disabled with `--no-live`.

Any Rust consumer can subscribe to the same stream via `MasterHandle`:

```rust
use _strobengine::distributed::master::{MasterConfig, MasterHandle};

let handle = MasterHandle::new(MasterConfig::parse())?; // parse() from clap
let mut live = handle.subscribe_live();
while let Ok(snapshot) = live.recv().await {
    println!(
        "{:.0} rps | {:.2}% err | p50 {:.1}ms p95 {:.1}ms p99 {:.1}ms | {} workers",
        snapshot.rps,
        snapshot.error_rate_percent,
        snapshot.p50_ms,
        snapshot.p95_ms,
        snapshot.p99_ms,
        snapshot.active_workers,
    );
}
```

`LiveSnapshot` fields: `run_id`, `epoch_ms`, `elapsed_secs`, `window_secs`,
`rps`, `error_rate_percent`, `p50_ms`, `p90_ms`, `p95_ms`, `p99_ms`,
`total_requests`, `active_workers`. The channel is lossy under backpressure —
a slow subscriber drops old snapshots (`RecvError::Lagged`) instead of stalling
the pipeline.

## Report format

The final report is a single JSON document. Write it to a file with
`--report report.json`, to stdout with `--report -`, or omit it to skip
persistence.

```json
{
  "test_run_id": "run-1750000000000",
  "workers": ["worker-1", "worker-2"],
  "worker_count": 2,
  "batches": 60,
  "total_requests": 258140,
  "success_count": 258140,
  "failure_count": 0,
  "error_rate_percent": 0.0,
  "bytes_received": 5242880,
  "rps": 8604.7,
  "duration_secs": 30.0,
  "latency": {
    "p50_ms": 0.53, "p90_ms": 0.65, "p95_ms": 0.67, "p99_ms": 0.74,
    "min_ms": 0.24, "max_ms": 4.39, "sample_count": 258140,
    "bucket_counts": [1240, 180000, 60000, 16000, 700, 200, 0, 0, 0, 0]
  },
  "error_breakdown": [
    { "error_class": "Http5xx", "count": 0 }
  ],
  "timeline": [
    { "elapsed_secs": 0.0, "epoch_ms": 1750000000000, "rps": 8100.2,
      "error_rate_percent": 0.0, "p50_ms": 0.51, "p95_ms": 0.64, "p99_ms": 0.70 }
  ],
  "worker_shares": [
    { "worker_id": "worker-1", "total_requests": 129070, "success_count": 129070,
      "failure_count": 0, "error_rate_percent": 0.0, "rps": 4302.3,
      "share_percent": 50.0, "p95_ms": 0.66 }
  ]
}
```

Key sections:

- `latency` — merged percentiles and the fixed 10-bucket histogram
  (`<1, 1-5, 5-10, 10-25, 25-50, 50-100, 100-250, 250-500, 500-1000, >1000` ms).
- `error_breakdown` — failures by class (`Http4xx`, `Http5xx`, `Timeout`, …).
- `timeline` — one point per epoch (per-second RPS, error rate, p50/p95/p99).
- `worker_shares` — per-worker throughput, error rate, and percentage of the
  total (`share_percent`, sorted by request count).

## Limitations & notes

- **HTTP targets only** in this release; the distributed executor drives the
  HTTP engine.
- **Fixed virtual users per worker** — the ramp-up schedule in
  `TestConfiguration` is not applied yet; each worker runs its configured VU
  count for the run duration.
- **One active run per master.** Starting a second run while one is in flight is
  rejected.
- **Cluster percentiles are approximate.** Workers ship fixed histogram buckets
  (not raw samples), so the master merges them element-wise and interpolates
  percentiles within the containing bucket.
- **Reconnection.** Workers reconnect to the master with exponential backoff and
  re-register after a dropped connection; a worker marked `DISCONNECTED` by the
  health sweep is re-added once it registers again.
- **No external dependencies.** Everything runs over gRPC on your own network;
  point `--run-url` at your target and workers dial the master.
