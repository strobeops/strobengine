"""SSE (Server-Sent Events) load testing examples.

Demonstrates SSE streaming with stateless and persistent modes, custom headers,
max events, and chaos injection. Requires an SSE endpoint, e.g.:
  python -m aiohttp.web examples._sse_server  (see tests/e2e/mock_server.py)

Or use any HTTP endpoint that returns Content-Type: text/event-stream.
"""

from strobengine import RequestOptions, StrobEngine
from strobengine.reporter import print_summary

SSE_URL = "http://localhost:8080/sse"


def basic_sse_test():
    """Minimal SSE load test -- connects, reads one event per iteration."""
    engine = StrobEngine(
        url=SSE_URL,
        concurrency=10,
        duration=10,
        options=RequestOptions(sse_enabled=True),
    )
    summary = engine.run()
    print_summary(summary)


def sse_with_custom_headers():
    """SSE with authentication and custom headers."""
    engine = StrobEngine(
        url=SSE_URL,
        concurrency=5,
        duration=10,
        options=RequestOptions(
            sse_enabled=True,
            headers=[
                ("Authorization", "Bearer token123"),
                ("X-Request-Id", "load-test-001"),
            ],
        ),
    )
    summary = engine.run()
    print_summary(summary)


def sse_max_events():
    """SSE with a per-connection event cap."""
    engine = StrobEngine(
        url=SSE_URL,
        concurrency=5,
        duration=10,
        options=RequestOptions(
            sse_enabled=True,
            sse_max_events=50,
        ),
    )
    summary = engine.run()
    print_summary(summary)


def sse_scheme_url():
    """SSE using the sse:// URL scheme (auto-normalized to http://)."""
    engine = StrobEngine(
        url="sse://localhost:8080/sse",
        concurrency=5,
        duration=10,
    )
    summary = engine.run()
    print_summary(summary)


def sse_with_chaos():
    """SSE with chaos fault injection (latency spikes, connection drops)."""
    engine = StrobEngine(
        url=SSE_URL,
        concurrency=10,
        duration=10,
        options=RequestOptions(
            sse_enabled=True,
            chaos=True,
        ),
    )
    summary = engine.run()
    print_summary(summary)


def result_analysis():
    """Inspect TestSummary fields after an SSE test."""
    engine = StrobEngine(
        url=SSE_URL,
        concurrency=5,
        duration=5,
        options=RequestOptions(sse_enabled=True),
    )
    summary = engine.run()

    print(f"Total requests:      {summary.total_requests}")
    print(f"Total errors:        {summary.total_errors}")
    print(f"Avg latency:         {summary.average_latency_ms:.2f} ms")
    print(f"P50 latency:         {summary.p50_latency_ms:.2f} ms")
    print(f"P95 latency:         {summary.p95_latency_ms:.2f} ms")
    print(f"P99 latency:         {summary.p99_latency_ms:.2f} ms")
    print(f"Bytes received:      {summary.total_bytes_received}")
    print(f"Status codes:        {summary.status_codes}")


if __name__ == "__main__":
    basic_sse_test()
    sse_with_custom_headers()
    sse_max_events()
    sse_scheme_url()
    sse_with_chaos()
    result_analysis()
