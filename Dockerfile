# Stage 1: Build wheel
FROM python:3.11-slim AS builder
RUN apt-get update && apt-get install -y curl build-essential
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
ENV PATH="/root/.cargo/bin:${PATH}"
RUN pip install maturin

WORKDIR /app
COPY . .
RUN maturin build --release --out dist

# Stage 2: Minimal Runtime Image
FROM python:3.11-slim

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
