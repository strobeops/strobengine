"""Report artifact construction: the single place that turns a ``TestSummary``
(+ config) into the ``ReportArtifact`` JSON dict.

This isolates the PyO3 boundary: :func:`build_artifact_dict` delegates to the
native ``build_report_artifact_dict`` for a Rust ``TestConfig`` (guaranteeing
schema parity) and falls back to a pure-Python builder for ``RequestOptions``
(profile tests). The ``_format_*`` helpers render the optional protocol metric
blocks (``quic``/``sse``/``websocket``/``grpc``/``http3``/``system_metrics``/
``connection_pool``) and their key layout must stay in lock-step with the Rust
``ReportArtifact`` serde output.

Both paths follow the contract in :mod:`strobengine.report_schema`: optional
top-level blocks are omitted entirely when the run has no such metrics
(never ``null``), and numeric values are stored at full precision to mirror
``report/schema.rs`` (see PR #161 for the RPS precision stance).
"""

from __future__ import annotations

import sys
from typing import cast

from strobengine._strobengine import (
    GrpcMetrics,
    Http3Metrics,
    QuicMetrics,
    SseMetrics,
    SystemMetrics,
    TestSummary,
    WebsocketMetrics,
)
from strobengine.report_schema import (
    CliOptionsDict,
    ConnectionPoolDict,
    GrpcMetricsDict,
    Http3MetricsDict,
    QuicMetricsDict,
    ReportArtifactDict,
    SseMetricsDict,
    SystemInfoDict,
    SystemMetricsDict,
    WebsocketMetricsDict,
)


def _get_system_info() -> SystemInfoDict:
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


def build_artifact_dict(summary: TestSummary, config: object) -> ReportArtifactDict:
    """Build a ReportArtifact dict matching the Rust schema in report/schema.rs.

    When config is a Rust TestConfig, delegates to Rust for guaranteed 1:1 parity.
    Falls back to manual construction for Python RequestOptions (profile tests
    without TestConfig).
    """
    from strobengine._strobengine import (
        TestConfig as RustTestConfig,
        build_report_artifact_dict,
    )

    if isinstance(config, RustTestConfig):
        return cast(ReportArtifactDict, build_report_artifact_dict(summary, config))

    # Fallback: config is a Python RequestOptions (profile tests without TestConfig)
    return _build_artifact_dict_fallback(summary, config)


def _format_system_metrics(summary: TestSummary) -> SystemMetricsDict | None:
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


def _format_connection_pool(summary: TestSummary) -> ConnectionPoolDict | None:
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
        "socket_creation_rate": 1.0 - reuse_ratio,
        "socket_reuse_rate": reuse_ratio,
        "dns_lookup_ms": dns_ms,
    }


def _format_quic(summary: TestSummary) -> QuicMetricsDict | None:
    """Extract QUIC metrics from summary into a JSON-native dict or None."""
    q = getattr(summary, "quic", None)
    if q is None or not isinstance(q, QuicMetrics):
        return None
    return {
        "zero_rtt_accepted_count": q.zero_rtt_accepted_count,
        "retransmissions": q.retransmissions,
        "avg_handshake_ms": q.avg_handshake_ms,
    }


def _format_sse(summary: TestSummary) -> SseMetricsDict | None:
    """Extract SSE metrics from summary into a JSON-native dict or None."""
    s = getattr(summary, "sse", None)
    if s is None or not isinstance(s, SseMetrics):
        return None
    return {
        "total_events_received": s.total_events_received,
        "avg_ttfb_ms": s.avg_ttfb_ms,
    }


def _format_ws(summary: TestSummary) -> WebsocketMetricsDict | None:
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
        "backpressure_mean_bytes": w.backpressure_mean_bytes,
        "backpressure_threshold_breaches": w.backpressure_threshold_breaches,
    }


def _format_grpc(summary: TestSummary) -> GrpcMetricsDict | None:
    """Extract gRPC stream-concurrency/flow-control metrics into a JSON-native dict."""
    g = getattr(summary, "grpc", None)
    if g is None or not isinstance(g, GrpcMetrics):
        return None
    return {
        "active_streams_peak": g.active_streams_peak,
        "concurrency_utilization_peak": g.concurrency_utilization_peak,
        "concurrency_utilization_mean": g.concurrency_utilization_mean,
        "window_exhaustion_events_total": g.window_exhaustion_events_total,
        "window_stall_duration_ms_total": g.window_stall_duration_ms_total,
        "send_capacity_min_bytes": g.send_capacity_min_bytes,
    }


def _format_http3(summary: TestSummary) -> Http3MetricsDict | None:
    """Extract HTTP/3 congestion-window/migration metrics into a JSON-native dict."""
    h = getattr(summary, "http3", None)
    if h is None or not isinstance(h, Http3Metrics):
        return None
    return {
        "cwnd_bytes_current": h.cwnd_bytes_current,
        "cwnd_bytes_min": h.cwnd_bytes_min,
        "cwnd_bytes_max": h.cwnd_bytes_max,
        "cwnd_bytes_mean": h.cwnd_bytes_mean,
        "migrations_attempted_total": h.migrations_attempted_total,
        "migrations_successful_total": h.migrations_successful_total,
        "migration_success_rate": h.migration_success_rate,
    }


def _build_artifact_dict_fallback(
    summary: TestSummary, config: object
) -> ReportArtifactDict:
    """Manual construction for RequestOptions when TestConfig is unavailable.

    Mirrors ``ReportArtifact::from_summary_and_config`` in ``report/schema.rs``:
    conditional top-level blocks are added only when applicable (omit, never
    ``null``) and numerics are stored at full precision.
    """
    successful = max(summary.total_requests - summary.total_errors, 0)
    rps = (
        summary.total_requests / summary.duration_secs
        if summary.duration_secs > 0
        else 0.0
    )

    # Extract CLI options from config
    cli_options: CliOptionsDict = {
        "method": getattr(config, "method", "GET"),
        "concurrency": getattr(config, "concurrency", 0),
        "timeout_secs": getattr(config, "timeout_secs", 0),
        "chaos": getattr(config, "chaos", False),
        "chaos_rate": getattr(config, "chaos_rate", 0.1),
        "body": getattr(config, "body", None),
        "headers": getattr(config, "headers", None),
    }

    artifact: ReportArtifactDict = {
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
            "rps": rps,
            "bytes_transferred": summary.total_bytes_received,
        },
        "latency_percentiles": {
            "p50_us": summary.p50_latency_ms * 1000.0,
            "p90_us": summary.p90_latency_ms * 1000.0,
            "p95_us": summary.p95_latency_ms * 1000.0,
            "p99_us": summary.p99_latency_ms * 1000.0,
            "p99_99_us": summary.p99_99_latency_ms * 1000.0,
            "min_us": summary.min_latency_ms * 1000.0,
            "max_us": summary.max_latency_ms * 1000.0,
            "mean_us": summary.average_latency_ms * 1000.0,
            "std_dev_us": summary.std_dev_latency_ms * 1000.0,
        },
        "latency_histogram": summary.latency_histogram,
        "error_breakdown": {str(k): v for k, v in summary.status_codes.items()},
    }

    # Optional top-level blocks: omitted entirely when absent (report/schema.rs
    # uses skip_serializing_if, so the Rust path never emits null either).
    avg_conn = getattr(summary, "avg_connection_latency_us", 0.0)
    if isinstance(avg_conn, (int, float)) and avg_conn > 0.0:
        artifact["avg_connection_latency_us"] = avg_conn

    quic = _format_quic(summary)
    if quic is not None:
        artifact["quic"] = quic

    sse = _format_sse(summary)
    if sse is not None:
        artifact["sse"] = sse

    websocket = _format_ws(summary)
    if websocket is not None:
        artifact["websocket"] = websocket

    grpc = _format_grpc(summary)
    if grpc is not None:
        artifact["grpc"] = grpc

    http3 = _format_http3(summary)
    if http3 is not None:
        artifact["http3"] = http3

    chaos_total = getattr(summary, "chaos_injected_total", 0)
    if chaos_total:
        artifact["chaos_faults"] = {
            "injected_total": chaos_total,
            "by_type": getattr(summary, "chaos_faults_by_type", {}),
        }

    system_metrics = _format_system_metrics(summary)
    if system_metrics is not None:
        artifact["system_metrics"] = system_metrics

    connection_pool = _format_connection_pool(summary)
    if connection_pool is not None:
        artifact["connection_pool"] = connection_pool

    return artifact
