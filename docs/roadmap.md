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
- [ ] ~~Improved streaming metrics collection to minimize memory overhead during long runs.~~ *(Deferred until $O(1)$ memory metrics are required for multi-hour runs)*

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

## Multi-Target Parallel Execution

### Epic: Multi-Target Parallel Execution
*Target Focus: Concurrent Load Generation & Scripted Parallelism*

- [x] Intra-test worker task concurrency (`-c`/`--workers` mapped to Tokio async threads). `[v0.1.0]`
- [ ] Multi-target script orchestrator (`asyncio.gather` execution across multiple concurrent `StrobEngine` instances).
- [ ] Native multi-URL target parsing & concurrent execution via CLI.
- [ ] Process-level parallel execution runner integration (`pytest-xdist` parallel test pool orchestration).

---

## Reporting, Storage & Analytics

### Epic: Test Persistence & Visual Reporting
*Target Focus: Long-Term Benchmark Tracking & Visualization*

- [ ] **Artifact Persistence Engine**
  - [ ] Automatic disk storage for raw JSON execution metrics (`--output-dir`, `--save-report`).
  - [ ] Standardized report schema featuring execution metadata (timestamps, CLI flags, system specs, latency percentiles).
- [ ] **HTML / Visual Report Generator**
  - [ ] Standalone HTML report output with embedded latency distribution charts (p50, p90, p95, p99).
  - [ ] Summary dashboards comparing current runs against historical baseline artifacts.
- [ ] **Export Formats**
  - [ ] Markdown summary generation for CI/CD Pull Request integration (GitHub Actions step comments).
  - [ ] CSV/JUnit XML exports for standard test runner ingestion.

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
  - [x] Protobuf service definition parsing (base64/hex payloads, .proto file parsing, server reflection). `[v0.4.0] - 2026-08-23`
  - [x] Unary RPC calls with deadline support and chaos injection. `[v0.4.0] - 2026-08-23`
- [x] **HTTP/3 (QUIC)** `[v0.4.0] - 2026-08-23`
  - [x] UDP-based QUIC transport layer support. `[v0.4.0] - 2026-08-23`
  - [x] Zero-RTT connection testing and loss recovery benchmarking. `[v0.4.0] - 2026-08-23`
- [x] **Server-Sent Events (SSE / HTTP Streaming)** `[v0.5.0]`
  - [x] Persistent HTTP streaming response parsing (Time-to-First-Byte, chunk latency, LLM stream benchmarking). `[v0.5.0]`

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
