# Stage 1: Build wheel
FROM python:3.11-slim AS base
RUN apt-get update && apt-get install -y curl build-essential
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
ENV PATH="/root/.cargo/bin:${PATH}"
RUN pip install maturin

WORKDIR /app

RUN pip install --no-cache-dir "maturin>=1.0,<2.0" \
    && cargo install cargo-chef --locked

FROM base AS planner

COPY . .
RUN cargo chef prepare --recipe-path recipe.json

FROM base AS builder

COPY --from=planner /app/recipe.json recipe.json

RUN cargo chef cook --release \
    --features pyo3/extension-module \
    --recipe-path recipe.json

COPY . .
RUN maturin build --release --out dist

# Stage 2: Minimal Runtime Image
FROM python:3.11-slim AS runtime

# Create a non-privileged system user and group
RUN groupadd -r appuser && useradd -r -g appuser appuser

WORKDIR /app

# Copy built wheels with proper ownership
COPY --from=builder /app/dist/*.whl .
RUN pip install --no-cache-dir *.whl && rm *.whl

# Switch execution context away from root
USER appuser

ENTRYPOINT ["strobengine"]
CMD ["--help"]
