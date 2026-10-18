# Documentation

## Overview

- [README](../README.md) -- Project overview, quick start, and links
- [Installation](install.md) -- Build from source, Docker, PyPI
- [Dependencies](dependencies.md) -- System requirements, Rust crates, Python packages

## Features

- [Quick Start](quickstart.md) -- Python API and CLI usage examples
- [Examples](../examples/README.md) -- Runnable snippets for every supported protocol
- [CLI Reference](cli.md) -- All subcommands, flags, and options
- [HTTP Methods](http_methods.md) -- Supported HTTP methods, request bodies, headers, performance
- [WebSocket Load Testing](websockets.md) -- WS/WSS modes, pub/sub broadcasting, custom payloads, headers, metrics
- [gRPC Load Testing](grpc.md) -- Unary calls, protobuf payloads, deadline, chaos
- [HTTP/3 Load Testing](http3.md) -- QUIC transport, 0-RTT resumption, loss recovery metrics
- [SSE Load Testing](sse.md) -- Server-Sent Events streaming, TTFB, event intervals, max events
- [GraphQL Load Testing](graphql.md) -- Queries/mutations over HTTP, variables, logical error tracking
- [SOAP Load Testing](soap.md) -- SOAP 1.1/1.2 envelopes over HTTP, fault detection
- [JMS / Messaging Load Testing](jms.md) -- STOMP broker produce/consume throughput, ack latency, delivery intervals
- [Database (SQL) Load Testing](database.md) -- SQL connection pooling and query load (PostgreSQL, MySQL, SQLite)
- [Reporting & Visualizations](reports.md) -- JSON persistence, HTML reports, baseline comparison, output control
- [Distributed Load Testing](distributed.md) -- Master/worker control plane, live telemetry, cluster reporting

## Operations

- [Docker](docker.md) -- Pulling images, running containers, host networking, building locally

## Development

- [Testing](testing.md) -- Test structure, running tests, e2e mock server, CI/CD checks
- [Infrastructure](infrastructure.md) -- Build system, dependencies, architecture
- [Roadmap](roadmap.md) -- Planned features and project direction
- [Benchmarks](benchmarks.md) -- Performance benchmarks and methodology
- [Release](release.md) -- Maintainer releases and versioning workflows
