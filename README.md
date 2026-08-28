# strobengine

A high-performance HTTP, WebSocket, gRPC, and SSE load testing engine with a Python API and a bare-metal Rust core.

## Install

```bash
pip install strobengine
# or from source
git clone https://github.com/strobeops/strobengine.git && cd strobengine && uv sync
```

> See [Installation Guide](https://github.com/strobeops/strobengine/blob/main/docs/install.md) for Docker, PyPI, and development setup.

## Quick Start

```python
from strobengine import StrobEngine
from strobengine.reporter import print_summary

engine = StrobEngine(url="http://localhost:8080/api/health", concurrency=50, duration=30)
summary = engine.run()
print_summary(summary)
```

```bash
strobengine load http://localhost:8080/api/health -c 50 -d 30
```

> See [Quick Start Guide](https://github.com/strobeops/strobengine/blob/main/docs/quickstart.md) for Python API and CLI examples.

## Documentation

| Section | Description |
|---------|-------------|
| [Installation](https://github.com/strobeops/strobengine/blob/main/docs/install.md) | Build from source, Docker, PyPI |
| [Dependencies](https://github.com/strobeops/strobengine/blob/main/docs/dependencies.md) | System requirements, Rust crates, Python packages |
| [Quick Start](https://github.com/strobeops/strobengine/blob/main/docs/quickstart.md) | Python API and CLI usage examples |
| [CLI Reference](https://github.com/strobeops/strobengine/blob/main/docs/cli.md) | All subcommands, flags, and options |
| [HTTP Methods](https://github.com/strobeops/strobengine/blob/main/docs/http_methods.md) | Supported methods, request bodies, headers |
| [WebSocket](https://github.com/strobeops/strobengine/blob/main/docs/websockets.md) | WS/WSS modes, pub/sub broadcasting, metrics |
| [gRPC](https://github.com/strobeops/strobengine/blob/main/docs/grpc.md) | Unary calls, protobuf, deadline, chaos |
| [HTTP/3](https://github.com/strobeops/strobengine/blob/main/docs/http3.md) | QUIC transport, 0-RTT resumption, loss recovery |
| [SSE](https://github.com/strobeops/strobengine/blob/main/docs/sse.md) | Server-Sent Events streaming, TTFB, event intervals |
| [Reporting](https://github.com/strobeops/strobengine/blob/main/docs/reports.md) | JSON persistence, HTML reports, baseline comparison |
| [Docker](https://github.com/strobeops/strobengine/blob/main/docs/docker.md) | Pulling images, running containers |
| [Testing](https://github.com/strobeops/strobengine/blob/main/docs/testing.md) | Test structure, e2e, CI/CD |
| [Roadmap](https://github.com/strobeops/strobengine/blob/main/docs/roadmap.md) | Planned features |

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

This project is licensed under the Apache License, Version 2.0. See [LICENSE](https://github.com/strobeops/strobengine/blob/main/LICENSE) for the full text.
