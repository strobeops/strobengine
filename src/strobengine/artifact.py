"""Report artifact construction: the single place that turns a ``TestSummary``
(+ config) into the ``ReportArtifact`` JSON dict.

This isolates the PyO3 boundary: :func:`build_artifact_dict` delegates to the
native ``build_report_artifact_dict`` for a Rust ``TestConfig`` (guaranteeing
schema parity) and falls back to a pure-Python builder for ``RequestOptions``
(profile tests). The ``_format_*`` helpers render the optional protocol metric
blocks (``quic``/``sse``/``websocket``/``grpc``/``http3``/``system_metrics``/
``connection_pool``) and their key layout must stay in lock-step with the Rust
``ReportArtifact`` serde output.
"""

from __future__ import annotations

import sys

from strobengine._strobengine import (
    GrpcMetrics,
    Http3Metrics,
    QuicMetrics,
    SseMetrics,
    SystemMetrics,
    TestSummary,
    WebsocketMetrics,
)


def _get_system_info() -> dict:
    """Collect basic system information for report metadata."""
    import socket
    from importlib.metadata import PackageNotFoundError, version

    try:
        pkg_version = version("strobengine")
    except PackageNotFoundError:
        pkg_version = "0.0.0-dev"

    return {
        "hostname": socket.gethostname(),
        "platform": sys.platform,
        "version": pkg_version,
    }


def build_artifact_dict(summary: TestSummary, config: object) -> dict:
    """Build a ReportArtifact dict matching the Rust schema in report/schema.rs.

    When config is a Rust TestConfig, delegates to Rust for guaranteed 1:1 parity.
    Falls back to manual construction for Python RequestOptions (profile tests).
    """
    from strobengine._strobengine import (
        TestConfig as RustTestConfig,
        build_report_artifact_dict,
    )

    if isinstance(config, RustTestConfig):
        return build_report_artifact_dict(summary, config)

    # Fallback: config is a Python RequestOptions (profile tests without TestConfig)
    return _build_artifact_dict_fallback(summary, config)


def _format_system_metrics(summary: TestSummary) -> dict | None:
    """Extract system_metrics from summary into JSON-friendly format."""
    sm = getattr(summary, "system_metrics", None)
    if sm is None or not isinstance(sm, SystemMetrics):
        return None
    return {
        "summary": {
            "peak_cpu_percent": sm.peak_cpu_percent,
            "avg_cpu_percent": sm.avg_cpu_percent,
            "peak_memory_mb": round(sm.peak_memory_rss_bytes / (1024 * 1024), 1),
            "avg_memory_mb": round(sm.avg_memory_rss_bytes / (1024 * 1024), 1),
            "peak_threads": sm.peak_thread_count,
        },
        "samples": [
            {
                "elapsed_sec": round(s.timestamp_us / 1_000_000.0, 1),
                "cpu_percent": s.cpu_usage_percent,
                "memory_mb": round(s.memory_rss_bytes / (1024 * 1024), 1),
                "threads": s.thread_count,
            }
            for s in sm.time_series
        ],
    }


def _format_connection_pool(summary: TestSummary) -> dict | None:
    """Extract connection pool metrics from summary into JSON-friendly format."""
    total = getattr(summary, "total_requests", 0)
    if not isinstance(total, (int, float)) or total <= 0:
        return None
    reuse_ratio = getattr(summary, "connection_reuse_ratio", None)
    dns_ms = getattr(summary, "avg_dns_resolution_ms", None)
    if not isinstance(reuse_ratio, (int, float)) or not isinstance(
        dns_ms, (int, float)
    ):
        return None
    return {
        "socket_creation_rate": round(1.0 - reuse_ratio, 4),
        "socket_reuse_rate": round(reuse_ratio, 4),
        "dns_lookup_ms": round(dns_ms, 3),
    }


def _format_quic(summary: TestSummary) -> dict | None:
    """Extract QUIC metrics from summary into a JSON-native dict or None."""
    q = getattr(summary, "quic", None)
    if q is None or not isinstance(q, QuicMetrics):
        return None
    return {
        "zero_rtt_accepted_count": q.zero_rtt_accepted_count,
        "retransmissions": q.retransmissions,
        "avg_handshake_ms": q.avg_handshake_ms,
    }


def _format_sse(summary: TestSummary) -> dict | None:
    """Extract SSE metrics from summary into a JSON-native dict or None."""
    s = getattr(summary, "sse", None)
    if s is None or not isinstance(s, SseMetrics):
        return None
    return {
        "total_events_received": s.total_events_received,
        "avg_ttfb_ms": s.avg_ttfb_ms,
    }


def _format_ws(summary: TestSummary) -> dict | None:
    """Extract WebSocket heartbeat/backpressure metrics into a JSON-native dict."""
    w = getattr(summary, "ws", None)
    if w is None or not isinstance(w, WebsocketMetrics):
        return None
    return {
        "pings_sent_total": w.pings_sent_total,
        "pings_received_total": w.pings_received_total,
        "pongs_solicited_total": w.pongs_solicited_total,
        "pongs_unsolicited_total": w.pongs_unsolicited_total,
        "backpressure_max_bytes": w.backpressure_max_bytes,
        "backpressure_mean_bytes": round(w.backpressure_mean_bytes, 1),
        "backpressure_threshold_breaches": w.backpressure_threshold_breaches,
    }


def _format_grpc(summary: TestSummary) -> dict | None:
    """Extract gRPC stream-concurrency/flow-control metrics into a JSON-native dict."""
    g = getattr(summary, "grpc", None)
    if g is None or not isinstance(g, GrpcMetrics):
        return None
    return {
        "active_streams_peak": g.active_streams_peak,
        "concurrency_utilization_peak": round(g.concurrency_utilization_peak, 4),
        "concurrency_utilization_mean": round(g.concurrency_utilization_mean, 4),
        "window_exhaustion_events_total": g.window_exhaustion_events_total,
        "window_stall_duration_ms_total": round(g.window_stall_duration_ms_total, 1),
        "send_capacity_min_bytes": g.send_capacity_min_bytes,
    }


def _format_http3(summary: TestSummary) -> dict | None:
    """Extract HTTP/3 congestion-window/migration metrics into a JSON-native dict."""
    h = getattr(summary, "http3", None)
    if h is None or not isinstance(h, Http3Metrics):
        return None
    return {
        "cwnd_bytes_current": h.cwnd_bytes_current,
        "cwnd_bytes_min": h.cwnd_bytes_min,
        "cwnd_bytes_max": h.cwnd_bytes_max,
        "cwnd_bytes_mean": round(h.cwnd_bytes_mean, 1),
        "migrations_attempted_total": h.migrations_attempted_total,
        "migrations_successful_total": h.migrations_successful_total,
        "migration_success_rate": round(h.migration_success_rate, 4),
    }


def _build_artifact_dict_fallback(summary: TestSummary, config: object) -> dict:
    """Manual construction for RequestOptions when TestConfig is unavailable."""
    successful = summary.total_requests - summary.total_errors
    rps = (
        summary.total_requests / summary.duration_secs
        if summary.duration_secs > 0
        else 0.0
    )

    # Extract CLI options from config
    cli_options = {
        "method": getattr(config, "method", "GET"),
        "concurrency": getattr(config, "concurrency", 0),
        "timeout_secs": getattr(config, "timeout_secs", 0),
        "chaos": getattr(config, "chaos", False),
        "chaos_rate": getattr(config, "chaos_rate", 0.1),
        "body": getattr(config, "body", None),
        "headers": getattr(config, "headers", None),
    }

    return {
        "metadata": {
            "timestamp": summary.timestamp,
            "duration_secs": summary.duration_secs,
            "target_url": summary.url,
            "cli_options": cli_options,
            "system_info": _get_system_info(),
        },
        "summary": {
            "total_requests": summary.total_requests,
            "successful_requests": successful,
            "failed_requests": summary.total_errors,
            "rps": round(rps, 2),
            "bytes_transferred": summary.total_bytes_received,
        },
        "latency_percentiles": {
            "p50_us": round(summary.p50_latency_ms * 1000.0, 1),
            "p90_us": round(summary.p90_latency_ms * 1000.0, 1),
            "p95_us": round(summary.p95_latency_ms * 1000.0, 1),
            "p99_us": round(summary.p99_latency_ms * 1000.0, 1),
            "p99_99_us": round(summary.p99_99_latency_ms * 1000.0, 1),
            "min_us": round(summary.min_latency_ms * 1000.0, 1),
            "max_us": round(summary.max_latency_ms * 1000.0, 1),
            "mean_us": round(summary.average_latency_ms * 1000.0, 1),
            "std_dev_us": round(summary.std_dev_latency_ms * 1000.0, 1),
        },
        "latency_histogram": summary.latency_histogram,
        "error_breakdown": {str(k): v for k, v in summary.status_codes.items()},
        "avg_connection_latency_us": summary.avg_connection_latency_us,
        "quic": _format_quic(summary),
        "sse": _format_sse(summary),
        "chaos_faults": {
            "injected_total": getattr(summary, "chaos_injected_total", 0),
            "by_type": getattr(summary, "chaos_faults_by_type", {}),
        },
        "system_metrics": _format_system_metrics(summary),
        "connection_pool": _format_connection_pool(summary),
        "websocket": _format_ws(summary),
        "grpc": _format_grpc(summary),
        "http3": _format_http3(summary),
    }
