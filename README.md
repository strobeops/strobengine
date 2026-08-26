# strobengine

A high-performance HTTP, WebSocket, and gRPC load testing engine with a Python API and a bare-metal Rust core.

## Install

```bash
pip install strobengine
# or from source
git clone https://github.com/strobeops/strobengine.git && cd strobengine && uv sync
```

> See [Installation Guide](docs/install.md) for Docker, PyPI, and development setup.

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

> See [Quick Start Guide](docs/quickstart.md) for Python API and CLI examples.

## Documentation

| Section | Description |
|---------|-------------|
| [Installation](docs/install.md) | Build from source, Docker, PyPI |
| [Dependencies](docs/dependencies.md) | System requirements, Rust crates, Python packages |
| [Quick Start](docs/quickstart.md) | Python API and CLI usage examples |
| [CLI Reference](docs/cli.md) | All subcommands, flags, and options |
| [HTTP Methods](docs/http_methods.md) | Supported methods, request bodies, headers |
| [WebSocket](docs/websockets.md) | WS/WSS modes, pub/sub broadcasting, metrics |
| [gRPC](docs/grpc.md) | Unary calls, protobuf, deadline, chaos |
| [HTTP/3](docs/http3.md) | QUIC transport, 0-RTT resumption, loss recovery |
| [Docker](docs/docker.md) | Pulling images, running containers |
| [Testing](docs/testing.md) | Test structure, e2e, CI/CD |
| [Roadmap](docs/roadmap.md) | Planned features |

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
