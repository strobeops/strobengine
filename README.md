# strobengine

A high-performance HTTP load testing engine with a Python API and a bare-metal Rust core.

## Dependencies

- **Python** >= 3.11
- **Rust** (stable, with `cargo`)
- **uv** (Python package manager)

### Rust crates

| Crate | Version | Purpose |
|-------|---------|---------|
| pyo3 | 0.29 | Python FFI bindings (stable ABI, abi3-py311) |
| reqwest | 0.13 | HTTP client with connection pooling |
| tokio | 1.53 | Multi-threaded async runtime |
| tokio-util | 0.7 | CancellationToken for graceful worker shutdown |
| tracing | 0.1 | Structured logging instrumentation |
| tracing-subscriber | 0.3 | Log formatting and output (stderr/file) |
| indicatif | 0.17 | Terminal progress bar rendering |
| fastrand | 2 | Fast random number generation for chaos injection |
| http | 1 | HTTP method types and header primitives |
| bytes | 1 | Zero-copy byte buffer for request payloads |

## Installation & Compilation

```bash
# Clone the repository
git clone https://github.com/riccione/strobengine.git
cd strobengine

# Build the native extension and install the package
uv sync
```

`uv sync` invokes [maturin](https://github.com/PyO3/maturin) under the hood, which compiles the Rust code into a native Python extension module and installs it into your virtual environment.

### Docker

Alternatively, pull and run strobengine directly from Docker Hub:

```bash
docker pull strobeops/strobengine:latest
docker run --rm -it strobeops/strobengine load http://host.docker.internal:8080/api/health -c 50 -d 30
```

> See [docs/docker.md](docs/docker.md) for full Docker documentation including
> version tags, host networking, and building locally.

## Quick Start Usage

```python
from strobengine import StrobEngine
from strobengine.reporter import print_summary

# Constant load test
engine = StrobEngine(url="http://localhost:8080/api/health", concurrency=50, duration=30)
summary = engine.run()

print_summary(summary)

# Ramp/stress test (10 -> 200 workers over 60s, hold 30s)
engine = StrobEngine.stress_test(
    "http://localhost:8080/api/health",
    start_concurrency=10, max_concurrency=200,
    ramp_duration=60, hold_duration=30,
    headers=[("Authorization", "Bearer token123")],
)
summary = engine.run()

print_summary(summary)

# Spike test (baseline 5 -> peak 500 -> back to 5)
engine = StrobEngine.spike_test(
    "http://localhost:8080/api/health",
    baseline=5, peak_concurrency=500,
    pre_spike_duration=5, spike_duration=10, post_spike_duration=5,
    headers=[("X-Custom", "value")],
)
summary = engine.run()

print_summary(summary)

# POST request with JSON body and custom headers
engine = StrobEngine(
    url="http://localhost:8080/api/data",
    method="POST",
    body='{"name": "test", "value": 42}',
    headers=[("Authorization", "Bearer token123")],
)
summary = engine.run()
```

For async contexts (FastAPI, Typer, etc.):

```python
summary = await engine.run_async()
```

## CLI Usage

```bash
# Constant load test (default subcommand)
strobengine http://localhost:8080/api/health -c 50 -d 30

# Ramp/stress test
strobengine stress http://localhost:8080/api/health --from 10 --to 500 --ramp 60 --hold 30

# Spike test
strobengine spike http://localhost:8080/api/health --baseline 5 --peak 1000 --pre-spike 5 --spike-duration 10 --post-spike 5

# JSON output for CI/CD
strobengine load http://localhost:8080/api/health --json

# Chaos/fault injection test (~10% of requests get faults)
strobengine load http://localhost:8080/api/health --chaos

# POST with JSON body
strobengine load http://localhost:8080/api/data --method POST --body '{"key": "val"}'

# PUT with custom headers
strobengine load http://localhost:8080/api/resource/1 \
  --method PUT --body '{"name": "updated"}' --header "Authorization: Bearer token"

# Multiple headers (repeatable -H flag)
strobengine load http://localhost:8080/api/data \
  --method POST --body '{"key": "val"}' \
  --header "Authorization: Bearer token" \
  --header "X-Request-ID: abc-123"

# POST with URL-encoded form data
strobengine load http://localhost:8080/api/data \
  --method POST --form "key1=value1&key2=value2"

# DELETE
strobengine load http://localhost:8080/api/resource/1 --method DELETE

# Verbose debug output
strobengine load http://localhost:8080/api/health -vv

# Quiet mode (suppress logs, keep progress bar)
strobengine load http://localhost:8080/api/health -q
```

By default, this spawns **10 concurrent workers** for **10 seconds** with a **10-second request timeout**. Results are displayed as a formatted table with total requests, errors, requests/sec, and latency percentiles (min, avg, p50, p90, p95, p99, max).

### Subcommands

| Subcommand | Description |
|------------|-------------|
| `load` | Constant load test (default if no subcommand given) |
| `stress` | Ramp from starting to target concurrency, then hold |
| `spike` | Baseline -> peak -> baseline |

### Load Subcommand Options

| Flag | Default | Description |
|------|---------|-------------|
| `-c`, `--concurrency` | `10` | Number of concurrent workers |
| `-d`, `--duration` | `10` | Duration in seconds |
| `-t`, `--timeout` | `10` | Per-request timeout in seconds |
| `--method` | `GET` | HTTP method (GET, POST, PUT, DELETE, PATCH, HEAD, OPTIONS) |
| `--body` | none | Request body (raw string) |
| `--form` | none | Form data body (e.g. key1=val1&key2=val2) |
| `--header` | none | Custom header key:value (repeatable) |
| `--chaos` | off | Enable fault injection (~10% of requests) |
| `--no-progress` | off | Suppress live progress bar |
| `-v`, `-vv`, `-vvv` | warn | Increase verbosity (INFO, DEBUG, TRACE) |
| `-q`, `--quiet` | off | Suppress all output |
| `--log-file <path>` | none | Write logs to file |
| `--json` | off | Output raw JSON instead of formatted table |

### Stress Subcommand Options

| Flag | Default | Description |
|------|---------|-------------|
| `--from` | `10` | Starting concurrency |
| `--to` | `200` | Target concurrency |
| `--ramp` | `60` | Ramp duration in seconds |
| `--hold` | `30` | Hold duration at target concurrency |
| `-t`, `--timeout` | `10` | Per-request timeout in seconds |
| `--method` | `GET` | HTTP method (GET, POST, PUT, DELETE, PATCH, HEAD, OPTIONS) |
| `--body` | none | Request body (raw string) |
| `--form` | none | Form data body (e.g. key1=val1&key2=val2) |
| `--header` | none | Custom header key:value (repeatable) |
| `--chaos` | off | Enable fault injection (~10% of requests) |
| `--no-progress` | off | Suppress live progress bar |
| `-v`, `-vv`, `-vvv` | warn | Increase verbosity (INFO, DEBUG, TRACE) |
| `-q`, `--quiet` | off | Suppress all output |
| `--log-file <path>` | none | Write logs to file |
| `--json` | off | Output raw JSON |

### Spike Subcommand Options

| Flag | Default | Description |
|------|---------|-------------|
| `--baseline` | `5` | Baseline concurrency |
| `--peak` | `500` | Peak concurrency |
| `--pre-spike` | `5` | Pre-spike duration in seconds |
| `--spike-duration` | `10` | Spike duration in seconds |
| `--post-spike` | `5` | Post-spike duration in seconds |
| `-t`, `--timeout` | `10` | Per-request timeout in seconds |
| `--method` | `GET` | HTTP method (GET, POST, PUT, DELETE, PATCH, HEAD, OPTIONS) |
| `--body` | none | Request body (raw string) |
| `--form` | none | Form data body (e.g. key1=val1&key2=val2) |
| `--header` | none | Custom header key:value (repeatable) |
| `--chaos` | off | Enable fault injection (~10% of requests) |
| `--no-progress` | off | Suppress live progress bar |
| `-v`, `-vv`, `-vvv` | warn | Increase verbosity (INFO, DEBUG, TRACE) |
| `-q`, `--quiet` | off | Suppress all output |
| `--log-file <path>` | none | Write logs to file |
| `--json` | off | Output raw JSON |

### Global Options

| Flag | Description |
|------|-------------|
| `-V`, `--version` | Show version and exit |

> **HTTP Methods, Bodies & Headers:** See [docs/http_methods.md](docs/http_methods.md) for detailed documentation on supported HTTP methods, request body handling, custom headers, and performance characteristics.

### Live Progress Bar

During test execution, a live progress bar displays on stderr with real-time telemetry:

```
⠋ [00:00:05] [==============>-------------] 40% | 1250 req/s | 20 VUs | 12 err | avg 4.2ms
```

- **RPS**: Instantaneous requests per second (sampled every 200ms)
- **VUs**: Active virtual users / concurrent workers
- **Errors**: Total error count
- **Avg latency**: Running average across all completed requests

The progress bar auto-detects non-TTY environments (CI/CD, piped output) and suppresses itself. Use `--no-progress` to explicitly disable it on interactive terminals.

### Verbosity Levels

| Flag | Level | Shows |
|------|-------|-------|
| (default) | `warn` | Errors and warnings only |
| `-v` | `info` | Engine start/stop, test configuration |
| `-vv` | `debug` | Worker spawn, HTTP errors, connection events |
| `-vvv` | `trace` | Per-request latency, status codes, chaos injection |
| `-q` | off | Suppress all log output (progress bar remains) |

Logs stream to **stderr** by default, keeping stdout clean for JSON output piping:

```bash
strobengine load http://localhost:8080/api/health -vv --json > results.json
```

## Architecture

strobengine separates configuration, execution, and metrics into clean Rust modules, exposed to Python via PyO3:

- **`config`** -- `TestConfig` for static load, `LoadProfile` enum for dynamic profiles (Constant, Ramp, Spike) with target concurrency interpolation.
- **`worker`** -- Async worker loops with method-aware request building, static payload reuse (Bytes), and zero-allocation header management via `ClientBuilder::default_headers()`.
- **`metrics`** -- Lock-free atomic counters (`AtomicUsize`) track total requests and errors without contention. An aggregator task collects raw latencies, then `calculate_summary` computes min, average, p50, p90, p95, p99, and max percentiles in Rust at bare-metal speed.
- **`chaos`** -- Protocol-agnostic fault injection engine with `ChaosEngine` evaluator and `ChaosFault` enum (LatencySpike, CorruptedPayload, MetadataCorruption, ConnectionDrop).
- **`progress`** -- Background Tokio render task sampling atomic metrics every 200ms, displaying live RPS, active VUs, and latency via indicatif.
- **Orchestrator** -- Supervisor task ticks every 200ms, calculates target concurrency from the active profile curve, spawns/aborts workers dynamically.

The Python GIL is released entirely via `py.detach()` during test execution, allowing the full Tokio thread pool to run concurrently without throttling Python.

## Testing

> See [docs/testing.md](docs/testing.md) for the full testing guide, including
> e2e tests, CI/CD checks, and running individual test suites.

## How to Contribute

1. Fork the repository
2. Create a feature branch (`git checkout -b feat/my-feature`)
3. Install dependencies and set up pre-push Git hooks:
   ```bash
   uv sync
   uv run pre-commit install --hook-type pre-push
   ```
4. Run formatting, linting, and tests manually (or let git push run them automatically):
   ```bash
   make check
   ```
5. Commit your changes following [Conventional Commits](https://www.conventionalcommits.org/)
6. Push to your branch and open a Pull Request

## License

This project is licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE) for the full text.
