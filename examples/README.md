# Examples

Runnable snippets for every protocol strobengine supports. Each file is
self-contained: a module docstring explains how to start a local target, and the
script runs a handful of scenarios ending in a `result_analysis()` summary.

```bash
# Run any example (a target server is usually required)
uv run python examples/graphql.py
```

Some examples need an external server (noted below); the **SQLite** and
**STOMP** examples can run entirely offline against a local broker/database.

| Example | Protocol | Docs | Target |
|---------|----------|------|--------|
| [`load_test.py`](load_test.py) | HTTP constant load | [Quick start](../docs/quickstart.md) | Any HTTP server |
| [`stress_test.py`](stress_test.py) | HTTP ramp/stress | [Quick start](../docs/quickstart.md) | Any HTTP server |
| [`spike_test.py`](spike_test.py) | HTTP spike | [Quick start](../docs/quickstart.md) | Any HTTP server |
| [`http_methods.py`](http_methods.py) | HTTP methods | [HTTP methods](../docs/http_methods.md) | go-httpbin (`docker.io/mccutchen/go-httpbin`) |
| [`websocket.py`](websocket.py) | WebSocket | [WebSockets](../docs/websockets.md) | WS echo server (`docker.io/websockets/echo`) |
| [`grpc.py`](grpc.py) | gRPC | [gRPC](../docs/grpc.md) | gRPC Greeter server |
| [`http3.py`](http3.py) | HTTP/3 (QUIC) | [HTTP/3](../docs/http3.md) | HTTP/3-capable server |
| [`sse.py`](sse.py) | Server-Sent Events | [SSE](../docs/sse.md) | SSE endpoint |
| [`graphql.py`](graphql.py) | GraphQL (HTTP + WS subscriptions) | [GraphQL](../docs/graphql.md) | GraphQL endpoint |
| [`soap.py`](soap.py) | SOAP 1.1 / 1.2 | [SOAP](../docs/soap.md) | SOAP endpoint |
| [`stomp.py`](stomp.py) | JMS / STOMP | [JMS / messaging](../docs/jms.md) | STOMP broker (`docker.io/rmohr/activemq`, port 61613) |
| [`database.py`](database.py) | SQL (PostgreSQL / MySQL / SQLite) | [Database](../docs/database.md) | SQLite (offline) or a SQL server |

Each example links the corresponding detailed guide in
[`docs/`](../docs/table_of_contents.md).
