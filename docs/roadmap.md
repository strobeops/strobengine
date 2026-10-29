# Roadmap

This document outlines the planned trajectory and upcoming feature epics for **strobengine**. 

> **Note:** Priorities and timelines may adjust based on community feedback, feature requests, and core architectural developments.

---

## Core Tooling & Distribution

### Epic: MVP Baseline Engine
- [x] High-throughput HTTP execution core (Rust async runtime). `[v0.1.0]`
- [x] Basic CLI flags (`-c`, `-d`, target URL). `[v0.1.0]`
- [x] Initial benchmark suite against k6. `[v0.1.0]`

---

### Epic: Binary Distribution & PyPI Publishing
*Target Focus: Packaging & Ecosystem Delivery*

- [x] Standalone binary compilation/build matrix for main target architectures. `[v0.1.0]`
- [x] PyPI package publishing workflow (`pip install strobengine`). `[v0.1.0]`
- [x] GitHub Actions release pipeline for automatic binaries attachment on tag creation. `[v0.1.0]`

---

### Epic: Fault Injection & Resilience Testing (Chaos)

- [x] Basic chaos testing support. `[v0.2.0]`

### Epic: Real-Time Telemetry & Progress Bars
*Target Focus: UX & Observability*

- [x] Interactive CLI progress indicators (live RPS, active virtual users, latency feed). `[v0.2.0]`
- [ ] ~~Real-time telemetry dashboard / TUI integration.~~ *(Postponed: Python scripting is primary runner; CLI is for quick drafts only)*
- [ ] ~~Improved streaming metrics collection to minimize memory overhead during long runs.~~ *(Completed: replaced unbounded Vec<u128> with hdrhistogram::Histogram<u64> for O(1) memory)*

---

### Epic: HTTP Customization
*Target Focus: Feature Completeness*

- [x] Custom HTTP Headers support (authentication, user-agents, metadata). `[v0.2.0]`
- [x] Dynamic & static Request Payloads (`POST`/`PUT`/`PATCH` body support with JSON/Form Data). `[v0.2.0]`
- [x] Support for all standard HTTP methods (`GET`, `POST`, `PUT`, `DELETE`, `PATCH`, `HEAD`, `OPTIONS`). `[v0.2.0]`

---

### Epic: Automated End-to-End Test Suite
*Target Focus: Quality Assurance & Reliability*

- [x] E2E integration tests against mock HTTP targets (error scenarios, timeouts, high concurrency). `[v0.3.0]`
- [ ] Benchmarking and performance regression tests in CI pipeline.
- [ ] Automated cross-platform CLI verification (Linux, macOS, Windows).

---

### Epic: Static Type Safety & Quality Assurance
*Target Focus: Codebase Maintainability & Developer Ergonomics*

- [x] Integrate Pyright static type checking into dev workflow (`uv add --dev pyright` / `uv run pyright`). `[v0.8.1]`
- [x] Configure `pyrightconfig.json` with basic strictness to enforce function signatures and PyO3 interface contracts across Python/Rust boundaries. `[v0.8.1]`
- [x] Add Pyright validation step to local checks and CI workflow. `[v0.8.1]`

---

## Multi-Target Parallel Execution

### Epic: Multi-Target Parallel Execution
*Target Focus: Concurrent Load Generation & Scripted Parallelism*

- [x] Intra-test worker task concurrency (`-c`/`--workers` mapped to Tokio async threads). `[v0.1.0]`
- [ ] Multi-target script orchestrator (`asyncio.gather` execution across multiple concurrent `StrobEngine` instances).
- [ ] Native multi-URL target parsing & concurrent execution via CLI.
- [ ] Process-level parallel execution runner integration (`pytest-xdist` parallel test pool orchestration).

---

## Runtime Execution Context

### Epic: Runtime Execution Context
*Target Focus: Per-iteration data injection, deterministic data, and safe lifecycle control for pytest-driven load*

*Priority: P1 — next milestone; P2 — later. P0 is reserved for release-blocking work.*

> Existing pieces: per-iteration timeouts/session shutdown and distributed STOP/ABORT plumbing are already present. The gaps are local graceful drain, a Python abort API, per-VU context/overrides, and deterministic data helpers.

- [ ] **Per-VU deterministic RNG** `[P1]`
  - [x] Add a run-level `test_seed` (`RequestOptions`/CLI `--seed`, `TestConfig`) and derive a stable seed from `(test_seed, vu_id, iter)`.
  - [ ] Expose `rng_for(vu_id, iter)` returning a `random.Random` with a stream stable across runs and processes.
  - [x] Document that this seeds Python-side fixture/data generation only; Rust chaos stays on `fastrand` and is not deterministic under this seed.
  - [ ] Test cross-run/process determinism and distinct per-VU data.

- [ ] **Graceful stop, drain, and abort** `[P1]`
  - [x] Existing: per-iteration timeout, worker/session shutdown, distributed STOP/ABORT command plumbing.
  - [ ] Stop scheduling new iterations and drain in-flight work with a configurable grace timeout on `graceful_stop`.
  - [ ] Treat `graceful_ramp_down` as in-flight drain when a profile or controller reduces concurrency (not a timed ramp-down).
  - [ ] Add `StrobEngine.abort(reason)`/a thread-safe abort handle usable from a watchdog thread, signal handler, or `pytest.exit()` path; immediate cancel with clean teardown.
  - [ ] Return a partial summary on graceful stop; define whether abort returns a partial summary or raises (e.g. `AbortedError`).
  - [ ] Unify local and distributed semantics; first SIGINT = graceful stop, second = abort.
  - [ ] Test drain, grace-timeout fallback, cross-thread abort, and partial metrics.

- [ ] **Per-VU iteration context and HTTP request overrides** `[P1]`
  - [ ] Add stable `vu_id` slots and per-VU `iter` counters for constant and dynamic profiles; include an optional run/scenario label.
  - [ ] Add an opt-in Python `on_iteration(context)` callback returning HTTP request overrides (`url`, `method`, `headers`, `body`, `form`).
  - [ ] Apply overrides in the HTTP engine; define callback exception handling and GIL/throughput caveats.
  - [ ] Expose context/overrides in the Python API and pyi; document the high-RPS cost and prefer precomputed data for throughput-sensitive runs.
  - [ ] Test per-VU/iter values, override application, dynamic-profile identity, and callback errors.

- [ ] **All-protocol overrides** `[P2]`
  - [ ] Extend per-iteration overrides to WebSocket, gRPC, GraphQL/SOAP, SSE, STOMP, and database engines where meaningful.
  - [ ] Define and validate protocol-specific override schemas.

- [ ] **Named scenario model** `[P2]`
  - [ ] Add named scenarios/groups beyond a single run label, with per-scenario context and data; align with multi-target orchestration.

---

## Reporting, Storage & Analytics

### Epic: Test Persistence & Visual Reporting
*Target Focus: Long-Term Benchmark Tracking & Visualization*

- [x] **Artifact Persistence Engine** `[v0.5.0]`
  - [x] Automatic disk storage for raw JSON execution metrics (`--output-dir`, `--no-save`). `[v0.5.0]`
  - [x] Standardized report schema featuring execution metadata (timestamps, CLI flags, system specs, latency percentiles). `[v0.5.0]`
- [x] **HTML / Visual Report Generator** `[v0.5.0]`
  - [x] Standalone HTML report output with embedded latency distribution charts (p50, p90, p95, p99). `[v0.5.0]`
  - [x] Summary dashboards comparing current runs against historical baseline artifacts. `[v0.5.0]`
- [x] **Export Formats** `[v0.5.0]`
  - [x] Markdown summary generation for CI/CD Pull Request integration (GitHub Actions step comments). `[v0.5.0]`
  - [x] CSV/JUnit XML exports for standard test runner ingestion. `[v0.5.0]`

---

### Epic: Advanced Metrics & Observability
*Target Focus: Deep Performance Insights & System Monitoring*

- [x] **Aggregate Existing Protocol Metrics** `[v0.6.0]`
  - [x] Forward `connection_latency_us`, `quic_*`, `sse_*` fields through aggregator to `TestSummary`
  - [x] Add protocol-specific sections to JSON report artifact

- [x] **Chaos Fault Tracking** `[v0.6.0]`
  - [x] Count injected faults by type (ConnectionDrop, LatencySpike, etc.)
  - [x] Include fault breakdown in JSON report artifact

- [x] **Extended Latency Statistics** `[v0.6.0]`
  - [x] Compute latency jitter (standard deviation)
  - [x] Add p99.99 percentile
  - [x] Add latency histogram buckets for distribution visualization

- [x] **Client System Resource Monitoring** `[v0.7.0]`
  - [x] Track engine CPU %, memory (RSS), thread count during test
  - [x] Sample at configurable interval via `--sys-sample-interval` (default 1000ms, 0 to disable)
  - [x] Include in JSON report artifact (`system_metrics` field) and HTML visualization (dual-axis Chart.js line chart)

- [x] **Connection Pool Metrics** `[v0.7.0]`
  - [x] Track socket creation rate vs reuse rate
  - [x] Report DNS resolution duration (if instrumentable)

- [ ] **Protocol-Specific Deep Metrics** `[v0.8.0]`
  - [x] WebSocket: unsolicited pings/pongs, backpressure depth
  - [x] gRPC: stream concurrency utilization, flow control window exhaustion
  - [x] HTTP/3: congestion window size, connection migration success rate

---

## Modern Web APIs

### Epic: Modern Web APIs
*Target Focus: Protocol Expansion*

- [x] **WebSockets (`ws://`, `wss://`)** `[v0.4.0] - 2026-08-23`
  - [x] Full-duplex connection handshakes. `[v0.4.0] - 2026-08-23`
  - [x] Three execution modes: handshake, ping-pong, stream. `[v0.4.0] - 2026-08-23`
  - [x] Custom headers and per-iteration timeout. `[v0.4.0] - 2026-08-23`
  - [x] Chaos injection (latency spikes, corrupted payloads, connection drops). `[v0.4.0] - 2026-08-23`
  - [x] Frame broadcasting and multi-message-per-connection streaming. `[v0.4.0] - 2026-08-23`
  - [x] Server-push and pub/sub patterns. `[v0.4.0] - 2026-08-23`
- [x] **gRPC** `[v0.4.0] - 2026-08-23`
  - [x] Protobuf service definition parsing (base64/hex payloads, .proto file parsing). `[v0.4.0] - 2026-08-23`
  - [x] Unary RPC calls with deadline support and chaos injection. `[v0.4.0] - 2026-08-23`
- [ ] **gRPC Server Reflection**
  - [ ] Discover services and messages via the reflection API; enables the hidden `--grpc-use-reflection` flag.
- [x] **HTTP/3 (QUIC)** `[v0.4.0] - 2026-08-23`
  - [x] UDP-based QUIC transport layer support. `[v0.4.0] - 2026-08-23`
  - [x] Zero-RTT connection testing and loss recovery benchmarking. `[v0.4.0] - 2026-08-23`
- [x] **Server-Sent Events (SSE / HTTP Streaming)** `[v0.5.0]`
  - [x] Persistent HTTP streaming response parsing (Time-to-First-Byte, chunk latency, LLM stream benchmarking). `[v0.5.0]`
- [x] **GraphQL Benchmarking**
  - [x] **Queries & Mutations**: Benchmark execution performance, complex nested
    query resolution, and payload sizes over standard HTTP.
  - [x] **Subscriptions**: Real-time event streaming and subscription load
    testing over WebSockets (`graphql-transport-ws` subprotocol).

---

### Epic: Distributed Load Testing
*Target Focus: Multi-Node Coordination & High-Scale Concurrency*

- [x] **Distributed Runner Architecture**
  - [x] **Master-Worker Coordination**: Orchestrate multi-node load test runs
    from a central controller node to generate massive traffic loads beyond
    single-machine thread/socket limits.
  - [x] **Aggregated Telemetry Sync**: Real-time metric synchronization, health
    checks, and unified final reporting from distributed worker nodes.

---

## Reporting, Storage & Analytics

### Epic: Advanced HTML Visual Reports & SLO Compliance
*Target Focus: Executive Summaries, Timeline Telemetry & Diagnostic Visibility*

- [x] **Timeline & Trend Visualization**
  - [x] **RPS & Latency Over Time Charts**: Dual-axis or synchronized timeline
    graphs tracking Requests per Second (RPS) alongside P50/P95/P99 latency
    trends to spot warm-up phases, throttling, or performance degradation
    mid-test.
- [x] **SLO & Threshold Validation**
  - [x] **Pass/Fail Status Banner**: Configurable Service Level Objective (SLO)
    thresholds (e.g., P95 latency < 50ms, Error Rate < 0.1%) rendered as an
    instant pass/fail badge at the top of the report.
- [x] **Deep-Dive Statistics & Diagnostics**
  - [x] **Detailed Latency Summary Table**: Comprehensive statistical breakdown
    listing exact values for Min, Mean, P50, P90, P95, P99, P99.9, and Max.
  - [x] **Error Breakdown Section**: Dedicated error classification block
    tracking specific failure modes (timeouts, connection resets, or HTTP 5xx
    status code subgroups) for easier debugging.
- [x] **Export & Sharing Ergonomics**
  - [x] **Print-Friendly CSS & Actions**: Embedded print styles and a "Print to
    PDF" button for seamless archiving, sharing in pull requests, or inclusion
    in technical documentation.

---

## Enterprise Integration & Data Protocols

### Epic: Enterprise Services & Messaging
*Target Focus: Legacy Web Services & Enterprise Messaging*

- [x] **SOAP Web Services**
  - [x] SOAP client integration and XML payload performance benchmarking.
- [x] **JMS (Java Message Service)**
  - [x] Enterprise messaging queue throughput and message broker benchmarking (via STOMP 1.2).

### Epic: Data Stores & Directory Services
*Target Focus: Relational, NoSQL, and Directory Systems*

- [x] **Database via JDBC**
  - [x] Standard relational database (SQL) connection pooling and query load testing (PostgreSQL, MySQL/MariaDB, SQLite via `sqlx`).
- [ ] **MongoDB**
  - [ ] NoSQL document storage ingestion and read/write performance benchmarking.
- [ ] **LDAP (Directory Services)**
  - [ ] Directory lookup, authentication, and authorization benchmarking.

### Epic: Communication & Automation
*Target Focus: Mail Protocols & Local Execution*

- [ ] **Mail Protocols**
  - [ ] **SMTP(S)**: Email delivery throughput and concurrency benchmarking.
  - [ ] **POP3(S) & IMAP(S)**: Email retrieval, mailbox monitoring, and storage load testing.
- [ ] **Native Commands & Shell Scripts**
  - [ ] Local system command execution and shell script automation benchmarking.

---

## Infrastructure & Streaming Protocols

### Epic: Event-Driven Infrastructure & Message Queues
*Target Focus: Async Microservices & IoT Protocols*

- [ ] **MQTT (IoT / Telemetry)**
  - [ ] Async MQTT client integration (`rumqttc`) for QoS 0/1/2 pub/sub benchmarking.
- [ ] **Event Brokers**
  - [ ] **Apache Kafka / Redpanda**: Producer load testing and partition ingestion benchmarking.
  - [ ] **RabbitMQ (AMQP)**: Queue message publish throughput and acknowledgment latencies.

---

### Epic: Infrastructure & Low-Level Transport
*Target Focus: Network Layers & Caching Tiers*

- [ ] **Low-Level Transport**
  - [ ] **Raw TCP / TLS Socket Streaming**: Custom binary payload socket streaming (`tokio::net::TcpStream`).
  - [ ] **UDP / QUIC Raw Sockets**: High-frequency datagram hammering, jitter evaluation, and packet loss tracking.
- [ ] **In-Memory Caching Protocols**
  - [ ] **Redis / RESP Protocol**: High-concurrency GET/SET operation benchmarking and connection pool stress testing.

---

## Contributing to the Roadmap

Have ideas for future milestones or improvements? Feel free to open a feature request via [GitHub Issues](../../issues) or join the discussion!
