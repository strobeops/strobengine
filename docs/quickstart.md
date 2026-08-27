# Quick Start

## Python API

### Constant Load Test

```python
from strobengine import StrobEngine
from strobengine.reporter import print_summary

engine = StrobEngine(url="http://localhost:8080/api/health", concurrency=50, duration=30)
summary = engine.run()
print_summary(summary)
```

### Ramp/Stress Test

```python
engine = StrobEngine.stress_test(
    "http://localhost:8080/api/health",
    start_concurrency=10,
    max_concurrency=200,
    ramp_duration=60,
    hold_duration=30,
)
summary = engine.run()
print_summary(summary)
```

### Spike Test

```python
engine = StrobEngine.spike_test(
    "http://localhost:8080/api/health",
    baseline=5,
    peak_concurrency=500,
    pre_spike_duration=5,
    spike_duration=10,
    post_spike_duration=5,
)
summary = engine.run()
print_summary(summary)
```

### POST with JSON Body

```python
from strobengine import StrobEngine, RequestOptions

engine = StrobEngine(
    url="http://localhost:8080/api/data",
    options=RequestOptions(
        method="POST",
        body='{"name": "test", "value": 42}',
        headers=[("Authorization", "Bearer token123")],
    ),
)
summary = engine.run()
print_summary(summary)
```

### gRPC Unary Call

```python
engine = StrobEngine(
    url="grpc://localhost:50051",
    concurrency=10,
    duration=30,
    options=RequestOptions(
        grpc_service="helloworld.Greeter",
        grpc_method="SayHello",
        grpc_payload="CgR0ZXN0",
    ),
)
summary = engine.run()
print_summary(summary)
```

### SSE Streaming

```python
engine = StrobEngine(
    url="http://localhost:8080/sse",
    concurrency=10,
    duration=30,
    options=RequestOptions(sse_enabled=True),
)
summary = engine.run()
print_summary(summary)
```

### Async Execution

For async contexts (FastAPI, Typer, etc.):

```python
summary = await engine.run_async()
```

## CLI Usage

```bash
# Constant load test (default subcommand)
strobengine http://localhost:8080/api/health -c 50 -d 30

# Ramp/stress test
strobengine stress http://localhost:8080/api/health --from 10 --to 500 --ramp 60 --hold 30

# Spike test
strobengine spike http://localhost:8080/api/health --baseline 5 --peak 1000 --pre-spike 5 --spike-duration 10 --post-spike 5

# JSON output for CI/CD
strobengine load http://localhost:8080/api/health --json

# Chaos/fault injection test (~10% of requests get faults)
strobengine load http://localhost:8080/api/health --chaos

# POST with JSON body
strobengine load http://localhost:8080/api/data --method POST --body '{"key": "val"}'

# gRPC unary call
strobengine load grpc://localhost:50051 \
  --grpc-service helloworld.Greeter \
  --grpc-method SayHello \
  --grpc-payload CgR0ZXN0 \
  -c 10 -d 30

# SSE streaming
strobengine load http://localhost:8080/sse --sse -c 10 -d 30
```

> See [CLI Reference](cli.md) for all available flags and options.
