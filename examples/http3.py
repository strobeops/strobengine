"""HTTP/3 (QUIC) load testing examples.

Demonstrates QUIC transport, 0-RTT resumption, and loss recovery metrics.
Requires an HTTP/3-capable server. See docs/http3.md for setup instructions.
"""

from strobengine import RequestOptions, StrobEngine
from strobengine.reporter import print_summary

H3_URL = "h3://localhost:443/api"


def basic_http3_test():
    """Simplest HTTP/3 call via QUIC transport."""
    engine = StrobEngine(
        url=H3_URL,
        concurrency=10,
        duration=30,
        options=RequestOptions(
            http3_enabled=True,
        ),
    )
    summary = engine.run()
    print_summary(summary)


def zero_rtt_test():
    """HTTP/3 with 0-RTT session resumption for fast reconnects."""
    engine = StrobEngine(
        url=H3_URL,
        concurrency=10,
        duration=30,
        options=RequestOptions(
            http3_enabled=True,
            quic_zero_rtt=True,
        ),
    )
    summary = engine.run()
    print_summary(summary)


def custom_headers_test():
    """HTTP/3 with authentication headers."""
    engine = StrobEngine(
        url=H3_URL,
        concurrency=10,
        duration=30,
        options=RequestOptions(
            http3_enabled=True,
            headers=[("Authorization", "Bearer token123")],
        ),
    )
    summary = engine.run()
    print_summary(summary)


def chaos_test():
    """HTTP/3 with chaos fault injection."""
    engine = StrobEngine(
        url=H3_URL,
        concurrency=10,
        duration=30,
        options=RequestOptions(
            http3_enabled=True,
            chaos=True,
        ),
    )
    summary = engine.run()
    print_summary(summary)


def result_analysis():
    """Inspect QUIC-specific metrics after an HTTP/3 test."""
    engine = StrobEngine(
        url=H3_URL,
        concurrency=5,
        duration=5,
        options=RequestOptions(
            timeout=3,
            http3_enabled=True,
        ),
    )
    summary = engine.run()

    print(f"Total requests:      {summary.total_requests}")
    print(f"Total errors:        {summary.total_errors}")
    print(f"Avg latency:         {summary.average_latency_ms:.2f} ms")
    print(f"P95 latency:         {summary.p95_latency_ms:.2f} ms")
    print(f"P99 latency:         {summary.p99_latency_ms:.2f} ms")

    print("\nStatus codes:")
    for code, count in sorted(summary.status_codes.items()):
        print(f"  {code}: {count} requests")


if __name__ == "__main__":
    basic_http3_test()
    zero_rtt_test()
    custom_headers_test()
    chaos_test()
    result_analysis()
