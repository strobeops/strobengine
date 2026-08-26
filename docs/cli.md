# CLI Reference

strobengine provides a CLI for running load tests from the terminal.

## Default Behavior

By default, this spawns **10 concurrent workers** for **10 seconds** with a **10-second request timeout**. Results are displayed as a formatted table with total requests, errors, requests/sec, and latency percentiles (min, avg, p50, p90, p95, p99, max).

## Subcommands

| Subcommand | Description |
|------------|-------------|
| `load` | Constant load test (default if no subcommand given) |
| `stress` | Ramp from starting to target concurrency, then hold |
| `spike` | Baseline -> peak -> baseline |

## Load Subcommand Options

| Flag | Default | Description |
|------|---------|-------------|
| `-c`, `--concurrency` | `10` | Number of concurrent workers |
| `-d`, `--duration` | `10` | Duration in seconds |
| `-t`, `--timeout` | `10` | Per-request timeout in seconds |
| `--method` | `GET` | HTTP method (GET, POST, PUT, DELETE, PATCH, HEAD, OPTIONS) |
| `--body` | none | Request body (raw string) |
| `--form` | none | Form data body (e.g. key1=val1&key2=val2) |
| `--header` | none | Custom header key:value (repeatable) |
| `--chaos` | off | Enable fault injection (~10%% of requests) |
| `--no-progress` | off | Suppress live progress bar |
| `-v`, `-vv`, `-vvv` | warn | Increase verbosity (INFO, DEBUG, TRACE) |
| `-q`, `--quiet` | off | Suppress all output |
| `--log-file <path>` | none | Write logs to file |
| `--json` | off | Output raw JSON instead of formatted table |
| `--ws-mode` | `handshake` | WebSocket mode: `handshake`, `ping_pong`, `stream` |
| `--ws-payload` | none | WebSocket text payload for stream mode |
| `--ws-role` | none | WebSocket Pub/Sub role: `publisher`, `subscriber` |
| `--ws-publish-interval` | none | Publisher send interval in milliseconds |
| `--ws-subscribers` | none | Number of subscriber workers |
| `--grpc-service` | none | gRPC service name (e.g. helloworld.Greeter) |
| `--grpc-method` | none | gRPC method name (e.g. SayHello) |
| `--grpc-payload` | none | Base64-encoded protobuf payload |
| `--grpc-deadline-ms` | none | gRPC deadline in milliseconds |
| `--http3/--no-http3` | off | Enable HTTP/3 over QUIC |
| `--quic-zero-rtt` | off | Enable QUIC 0-RTT connection testing |
| `--quic-max-idle-timeout` | none | QUIC max idle timeout in ms |

## Stress Subcommand Options

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
| `--chaos` | off | Enable fault injection (~10%% of requests) |
| `--no-progress` | off | Suppress live progress bar |
| `-v`, `-vv`, `-vvv` | warn | Increase verbosity (INFO, DEBUG, TRACE) |
| `-q`, `--quiet` | off | Suppress all output |
| `--log-file <path>` | none | Write logs to file |
| `--json` | off | Output raw JSON |
| `--ws-mode` | `handshake` | WebSocket mode: `handshake`, `ping_pong`, `stream` |
| `--ws-payload` | none | WebSocket text payload for stream mode |
| `--ws-role` | none | WebSocket Pub/Sub role: `publisher`, `subscriber` |
| `--ws-publish-interval` | none | Publisher send interval in milliseconds |
| `--ws-subscribers` | none | Number of subscriber workers |
| `--grpc-service` | none | gRPC service name (e.g. helloworld.Greeter) |
| `--grpc-method` | none | gRPC method name (e.g. SayHello) |
| `--grpc-payload` | none | Base64-encoded protobuf payload |
| `--grpc-deadline-ms` | none | gRPC deadline in milliseconds |
| `--http3/--no-http3` | off | Enable HTTP/3 over QUIC |
| `--quic-zero-rtt` | off | Enable QUIC 0-RTT connection testing |
| `--quic-max-idle-timeout` | none | QUIC max idle timeout in ms |

## Spike Subcommand Options

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
| `--chaos` | off | Enable fault injection (~10%% of requests) |
| `--no-progress` | off | Suppress live progress bar |
| `-v`, `-vv`, `-vvv` | warn | Increase verbosity (INFO, DEBUG, TRACE) |
| `-q`, `--quiet` | off | Suppress all output |
| `--log-file <path>` | none | Write logs to file |
| `--json` | off | Output raw JSON |
| `--ws-mode` | `handshake` | WebSocket mode: `handshake`, `ping_pong`, `stream` |
| `--ws-payload` | none | WebSocket text payload for stream mode |
| `--ws-role` | none | WebSocket Pub/Sub role: `publisher`, `subscriber` |
| `--ws-publish-interval` | none | Publisher send interval in milliseconds |
| `--ws-subscribers` | none | Number of subscriber workers |
| `--grpc-service` | none | gRPC service name (e.g. helloworld.Greeter) |
| `--grpc-method` | none | gRPC method name (e.g. SayHello) |
| `--grpc-payload` | none | Base64-encoded protobuf payload |
| `--grpc-deadline-ms` | none | gRPC deadline in milliseconds |
| `--http3/--no-http3` | off | Enable HTTP/3 over QUIC |
| `--quic-zero-rtt` | off | Enable QUIC 0-RTT connection testing |
| `--quic-max-idle-timeout` | none | QUIC max idle timeout in ms |

## Global Options

| Flag | Description |
|------|-------------|
| `-V`, `--version` | Show version and exit |

## Verbosity Levels

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

## Live Progress Bar

During test execution, a live progress bar displays on stderr with real-time telemetry:

```
⠋ [00:00:05] [==============>-------------] 40% | 1250 req/s | 20 VUs | 12 err | avg 4.2ms
```

- **RPS**: Instantaneous requests per second (sampled every 200ms)
- **VUs**: Active virtual users / concurrent workers
- **Errors**: Total error count
- **Avg latency**: Running average across all completed requests

The progress bar auto-detects non-TTY environments (CI/CD, piped output) and suppresses itself. Use `--no-progress` to explicitly disable it on interactive terminals.
