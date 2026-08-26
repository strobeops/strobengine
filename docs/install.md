# Installation

## From Source (Recommended)

```bash
# Clone the repository
git clone https://github.com/strobeops/strobengine.git
cd strobengine

# Build the native extension and install the package
uv sync
```

`uv sync` invokes [maturin](https://github.com/PyO3/maturin) under the hood, which compiles the Rust code into a native Python extension module and installs it into your virtual environment.

## From Docker

Pull and run strobengine directly from Docker Hub:

```bash
# Latest release
docker pull strobeops/strobengine:latest
docker run --rm -it strobeops/strobengine load http://host.docker.internal:8080/api/health -c 50 -d 30

# Specific version
docker pull strobeops/strobengine:0.3.0
```

> See [Docker documentation](docker.md) for version tags, host networking, and building locally.

## Verify Installation

```bash
# Check version
strobengine --version

# Or via Python
uv run python -c "from strobengine import StrobEngine; print('OK')"
```

## Development Setup

```bash
# Install with dev dependencies
uv sync

# Install pre-commit hooks (optional)
uv run pre-commit install --hook-type pre-push

# Run the full check suite
make check
```
