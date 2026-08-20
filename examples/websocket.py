"""WebSocket load testing examples.

Demonstrates handshake, ping-pong, stream modes, and pub/sub broadcasting.
Requires a WebSocket echo server, e.g.:
  podman run -d --rm -p 8080:8080 docker.io/websockets/echo

For pub/sub, use a broadcast-capable server (see tests/e2e/mock_server.py).
"""

from strobengine import RequestOptions, StrobEngine
from strobengine.reporter import print_summary

WS_URL = "ws://localhost:8080/ws"
BROADCAST_URL = "ws://localhost:8080/ws/broadcast"


def handshake_test():
    """Connect and immediately close -- measures connection latency."""
    engine = StrobEngine(
        url=WS_URL,
        concurrency=10,
        duration=10,
    )
    summary = engine.run()
    print_summary(summary)


def ping_pong_test():
    """Send Ping, wait for Pong -- measures round-trip time."""
    engine = StrobEngine(
        url=WS_URL,
        concurrency=10,
        duration=10,
        options=RequestOptions(ws_mode="ping_pong"),
    )
    summary = engine.run()
    print_summary(summary)


def stream_test():
    """Send a text payload, await response frame."""
    engine = StrobEngine(
        url=WS_URL,
        concurrency=10,
        duration=10,
        options=RequestOptions(
            ws_mode="stream",
            ws_payload='{"type": "echo", "data": "test"}',
        ),
    )
    summary = engine.run()
    print_summary(summary)


def custom_headers_test():
    """WebSocket with authentication headers."""
    engine = StrobEngine(
        url=WS_URL,
        concurrency=5,
        duration=10,
        options=RequestOptions(
            headers=[("Authorization", "Bearer token123")],
        ),
    )
    summary = engine.run()
    print_summary(summary)


def pubsub_publisher_test():
    """Publisher role -- sends timestamped binary frames each iteration."""
    engine = StrobEngine(
        url=BROADCAST_URL,
        concurrency=2,
        duration=10,
        options=RequestOptions(
            ws_role="publisher",
            ws_publish_interval_ms=200,
        ),
    )
    summary = engine.run()
    print_summary(summary)


def pubsub_subscriber_test():
    """Subscriber role -- receives broadcast frames, measures E2E latency."""
    engine = StrobEngine(
        url=BROADCAST_URL,
        concurrency=2,
        duration=10,
        options=RequestOptions(
            ws_role="subscriber",
            ws_subscribers=2,
        ),
    )
    summary = engine.run()
    print_summary(summary)
    if summary.avg_e2e_latency_us > 0:
        print(
            f"  Avg E2E broadcast latency: {summary.avg_e2e_latency_us / 1000:.2f} ms"
        )


def chaos_test():
    """WebSocket with chaos fault injection."""
    engine = StrobEngine(
        url=WS_URL,
        concurrency=10,
        duration=10,
        options=RequestOptions(
            ws_mode="stream",
            ws_payload="hello",
            chaos=True,
        ),
    )
    summary = engine.run()
    print_summary(summary)


def result_analysis():
    """Inspect TestSummary fields after a WebSocket test."""
    engine = StrobEngine(
        url=WS_URL,
        concurrency=5,
        duration=5,
        options=RequestOptions(ws_mode="stream", ws_payload="ping"),
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
    handshake_test()
    ping_pong_test()
    stream_test()
    custom_headers_test()
    pubsub_publisher_test()
    pubsub_subscriber_test()
    chaos_test()
    result_analysis()
