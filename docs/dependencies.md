# Dependencies

## System Requirements

- **Python** >= 3.11
- **Rust** stable (with `cargo`)
- **uv** (Python package manager)

## Rust Crates

| Crate | Version | Purpose |
|-------|---------|---------|
| pyo3 | 0.29 | Python FFI bindings (stable ABI, abi3-py311) |
| reqwest | 0.13 | HTTP client with connection pooling |
| tonic | 0.14 | gRPC framework with TLS support |
| quinn | 0.11 | QUIC transport for HTTP/3 |
| h3 | 0.0.8 | HTTP/3 protocol implementation |
| h3-quinn | 0.0.10 | h3-Quinn bridge |
| tokio | 1.53 | Multi-threaded async runtime |
| tokio-util | 0.7 | CancellationToken for graceful worker shutdown |
| tracing | 0.1 | Structured logging instrumentation |
| tracing-subscriber | 0.3 | Log formatting and output (stderr/file) |
| indicatif | 0.17 | Terminal progress bar rendering |
| fastrand | 2 | Fast random number generation for chaos injection |
| http | 1 | HTTP method types and header primitives |
| bytes | 1 | Zero-copy byte buffer for request payloads |
| tokio-tungstenite | 0.26 | WebSocket client with TLS support |
| prost / prost-reflect | 0.14 / 0.16 | Protobuf encoding/decoding and reflection |
| base64 | 0.22 | Base64 payload decoding for gRPC |
| hex | 0.4 | Hex payload decoding for gRPC |

## Python Dependencies

| Package | Version | Purpose |
|---------|---------|---------|
| pytest | >= 8.0 | Test runner |
| pytest-asyncio | >= 0.24 | Async test support |
| aiohttp | >= 3.14 | Mock server for E2E tests |
| ruff | >= 0.15 | Linting and formatting |
