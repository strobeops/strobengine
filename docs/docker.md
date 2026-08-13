# Docker

`strobengine` is available as an official Docker image for running load tests
without needing Python or Rust toolchains installed locally.

## Pulling the Image

```bash
# Latest release
docker pull strobeops/strobengine:latest

# Specific version
docker pull strobeops/strobengine:0.2.1

# Specific minor version
docker pull strobeops/strobengine:0.2
```

Images are published for both `linux/amd64` and `linux/arm64`.

## Running Load Tests

> **Note:** Pass `-it` when running commands interactively to allocate a
> pseudo-TTY so live progress bars display properly. Omit `-it` and use
> `--no-progress` in automated CI/CD environments.

```bash
# Show help
docker run --rm strobeops/strobengine --help

# Constant load test (50 workers, 30 seconds)
docker run --rm -it strobeops/strobengine load http://host.docker.internal:8080/api/health -c 50 -d 30

# Stress test (ramp 10 -> 200 over 60s, hold 30s)
docker run --rm -it strobeops/strobengine stress http://host.docker.internal:8080/api/health --from 10 --to 200 --ramp 60 --hold 30

# Spike test
docker run --rm -it strobeops/strobengine spike http://host.docker.internal:8080/api/health --baseline 5 --peak 500 --spike-duration 10

# CI / Non-interactive execution (JSON output)
docker run --rm strobeops/strobengine load http://host.docker.internal:8080/api/health -c 10 -d 5 --json --no-progress
```

## Accessing Host Services & Target Networks

Docker containers cannot reach `localhost` or `127.0.0.1` on your host machine
by default because `localhost` refers to the container itself.

- **macOS / Windows (Docker Desktop):**
  Use `host.docker.internal`:

  ```bash
  docker run --rm -it strobeops/strobengine load http://host.docker.internal:8080/get -c 20 -d 10
  ```

- **Linux (Host Networking):**
  Use `--network host` for minimal network overhead and direct access to host
  ports. This is recommended for high-throughput load tests on bare metal or
  cloud instances where container bridge networking may introduce socket overhead:

  ```bash
  docker run --rm --network host strobeops/strobengine load http://localhost:8080/api/health -c 20 -d 10
  ```

## Building Locally

To build and test the container image locally from source:

```bash
docker build -t strobengine .
docker run --rm strobengine --help
```

## Image Overview

| Property | Details |
|----------|---------|
| **Base Image** | `python:3.11-slim` |
| **Entrypoint** | `strobengine` |
| **User** | `appuser` (non-root) |
| **Architectures** | `linux/amd64`, `linux/arm64` |
| **Build Architecture** | Multi-stage (compiled PyO3 wheels copied to slim runtime) |

## CI/CD Pipeline

Official Docker Hub images are automatically built and published via GitHub
Actions on:

- Git tag push (`v*.*.*`)
- GitHub release publish
- Manual workflow dispatch (`workflow_dispatch`)

The automation tags images matching semantic version patterns (`latest`, `0.2`,
`0.2.1`) across both `linux/amd64` and `linux/arm64` architectures.
