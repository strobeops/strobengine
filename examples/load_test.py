"""Basic constant load test -- fires N concurrent GET requests for D seconds."""

import json

from strobengine import StrobEngine
from strobengine.reporter import print_summary


def load_test():
    # 50 concurrent workers hitting the endpoint for 30 seconds
    engine = StrobEngine(
        url="http://localhost:8080/get",
        concurrency=50,
        duration=30,
    )
    summary = engine.run()

    print_summary(summary, url=engine._url, duration_secs=30)


def load_test_json():
    engine = StrobEngine(
        url="http://localhost:8080/get",
    )
    summary = engine.run()

    data = {
        "url": engine._url,
        "total_requests": summary.total_requests,
        "total_errors": summary.total_errors,
        "average_latency_ms": summary.average_latency_ms,
        "p95_latency_ms": summary.p95_latency_ms,
        "p99_latency_ms": summary.p99_latency_ms,
        "min_latency_ms": summary.min_latency_ms,
        "p50_latency_ms": summary.p50_latency_ms,
        "p90_latency_ms": summary.p90_latency_ms,
        "max_latency_ms": summary.max_latency_ms,
        "total_bytes_received": summary.total_bytes_received,
        "duration_secs": summary.duration_secs,
        "workers": summary.workers,
        "timestamp": summary.timestamp,
        "raw_command": summary.raw_command,
        "status_codes": summary.status_codes,
    }
    print(json.dumps(data, indent=2))


load_test()
load_test_json()
