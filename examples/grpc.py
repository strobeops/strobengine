"""gRPC load testing examples.

Demonstrates unary RPC calls with base64 payloads, deadlines, and headers.
Requires a gRPC server, e.g. the Greeter example:
  # From grpc-go examples:
  go run examples/greeter/server/main.go

  # Or with grpc-java:
  ./gradlew :grpc-examples:helloWorldServer
"""

from strobengine import RequestOptions, StrobEngine
from strobengine.reporter import print_summary

GRPC_URL = "grpc://localhost:50051"
SERVICE = "helloworld.Greeter"
METHOD = "SayHello"
# base64-encoded protobuf: message HelloRequest { name = "test" }
PAYLOAD = "CgR0ZXN0"


def basic_unary_test():
    """Simplest gRPC unary call with base64 payload."""
    engine = StrobEngine(
        url=GRPC_URL,
        concurrency=10,
        duration=30,
        options=RequestOptions(
            grpc_service=SERVICE,
            grpc_method=METHOD,
            grpc_payload=PAYLOAD,
        ),
    )
    summary = engine.run()
    print_summary(summary)


def deadline_test():
    """gRPC call with deadline -- fails if server is slow."""
    engine = StrobEngine(
        url=GRPC_URL,
        concurrency=10,
        duration=30,
        options=RequestOptions(
            grpc_service=SERVICE,
            grpc_method=METHOD,
            grpc_payload=PAYLOAD,
            grpc_deadline_ms=5000,
        ),
    )
    summary = engine.run()
    print_summary(summary)


def custom_headers_test():
    """gRPC with authentication metadata."""
    engine = StrobEngine(
        url=GRPC_URL,
        concurrency=10,
        duration=30,
        options=RequestOptions(
            grpc_service=SERVICE,
            grpc_method=METHOD,
            grpc_payload=PAYLOAD,
            headers=[("authorization", "Bearer token123")],
        ),
    )
    summary = engine.run()
    print_summary(summary)


def chaos_test():
    """gRPC with chaos fault injection."""
    engine = StrobEngine(
        url=GRPC_URL,
        concurrency=10,
        duration=30,
        options=RequestOptions(
            grpc_service=SERVICE,
            grpc_method=METHOD,
            grpc_payload=PAYLOAD,
            chaos=True,
        ),
    )
    summary = engine.run()
    print_summary(summary)


# gRPC status codes are mapped to HTTP equivalents in the summary:
#   0  -> 200  (OK)
#   1  -> 499  (cancelled)
#   2  -> 500  (unknown)
#   3  -> 400  (invalid argument)
#   4  -> 504  (deadline exceeded)
#   5  -> 404  (not found)
#   7  -> 403  (permission denied)
#   8  -> 429  (resource exhausted)
#   13 -> 500  (internal)
#   14 -> 503  (unavailable)
#   16 -> 401  (unauthenticated)
GRPC_STATUS_MAP = {
    200: "OK",
    499: "CANCELLED",
    500: "INTERNAL / UNKNOWN",
    400: "INVALID_ARGUMENT",
    504: "DEADLINE_EXCEEDED",
    404: "NOT_FOUND",
    403: "PERMISSION_DENIED",
    429: "RESOURCE_EXHAUSTED",
    503: "UNAVAILABLE",
    401: "UNAUTHENTICATED",
}


def result_analysis():
    """Inspect TestSummary fields and interpret gRPC status codes."""
    engine = StrobEngine(
        url=GRPC_URL,
        concurrency=5,
        duration=5,
        options=RequestOptions(
            timeout=3,
            grpc_service=SERVICE,
            grpc_method=METHOD,
            grpc_payload=PAYLOAD,
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
        label = GRPC_STATUS_MAP.get(code, f"HTTP {code}")
        print(f"  {code} ({label}): {count} requests")


if __name__ == "__main__":
    basic_unary_test()
    deadline_test()
    custom_headers_test()
    chaos_test()
    result_analysis()
