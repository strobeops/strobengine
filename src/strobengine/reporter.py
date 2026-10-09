from __future__ import annotations

import os
import shutil
import sys
from dataclasses import dataclass

from strobengine._strobengine import (
    HISTOGRAM_BUCKET_ORDER,
    GrpcMetrics,
    Http3Metrics,
    SystemMetrics,
    TestSummary,
    WebsocketMetrics,
)

# build_artifact_dict now lives in strobengine.artifact. Re-exported here for
# backward compatibility (reporting/* and callers still import it from
# strobengine.reporter); prefer `from strobengine.artifact import ...` in new code.
from .artifact import build_artifact_dict

# save_report / DEFAULT_REPORT_DIR now live in strobengine.persistence.
# Re-exported for backward compatibility (engine/tests import save_report from
# strobengine.reporter); prefer `from strobengine.persistence import ...` in new code.
from .persistence import DEFAULT_REPORT_DIR, save_report  # noqa: F401  (re-export)

_HAS_RICH = False
try:
    from rich.console import Console
    from rich.table import Table

    _HAS_RICH = True
except ImportError:
    pass


def print_summary(
    summary: TestSummary,
    json_output: bool = False,
) -> None:
    if json_output:
        print(summary.to_json(indent=2))
        return

    if _HAS_RICH:
        _print_rich(summary)
    else:
        _print_plain(summary)
    _metrics_description()


def _format_number(n: int) -> str:
    return f"{n:,}"


def _format_bytes(n: int) -> str:
    units = ["B", "KB", "MB", "GB", "TB", "PB"]
    val = float(n)
    for unit in units:
        if val < 1024.0 or unit == units[-1]:
            if unit == "B":
                return f"{int(val)} B"
            return f"{val:.1f} {unit}"
        val /= 1024.0
    return f"{val:.1f} PB"


def _render_histogram(
    histogram: dict[str, int], max_width: int | None = None
) -> list[str]:
    """Render ASCII bar chart from latency histogram buckets."""
    if not histogram or not any(v > 0 for v in histogram.values()):
        return []

    # Dynamic width based on terminal size
    if max_width is None:
        terminal_width = shutil.get_terminal_size((80, 24)).columns
        # Reserve space for label (12) + separator (2) + count (~8) + padding (2)
        max_width = max(10, terminal_width - 24)

    max_count = max(histogram.values())
    if max_count == 0:
        max_count = 1

    lines = ["  Latency Distribution:"]
    for bucket in HISTOGRAM_BUCKET_ORDER:
        count = histogram.get(bucket, 0)
        if count == 0:
            continue
        bar_len = int((count / max_count) * max_width)
        bar = "█" * max(1, bar_len)
        lines.append(f"    {bucket:<12s}: {bar} {_format_number(count)}")

    return lines


def _error_rate(total: int, errors: int) -> str:
    if total == 0:
        return "0.00%"
    return f"{errors / total * 100:.2f}%"


def _format_status_codes(codes: dict[int, int]) -> str:
    if not codes:
        return "none"
    parts = []
    for code, count in sorted(codes.items()):
        if code == 0:
            parts.append(f"{code} (conn fail/timeout): {count}")
        else:
            parts.append(f"{code}: {count}")
    return " | ".join(parts)


# gRPC status codes the engines surface as HTTP-mapped equivalents:
# 499 = Cancelled, 504 = DeadlineExceeded.
_GRPC_MAPPED_HTTP_CODES: frozenset[int] = frozenset({499, 504})


def _is_grpc_mapped(codes: dict[int, int]) -> bool:
    """Check if status codes contain gRPC-mapped HTTP equivalents."""
    return any(code in _GRPC_MAPPED_HTTP_CODES for code in codes)


@dataclass(frozen=True)
class _Row:
    label: str
    value: str = ""
    style: str = ""
    indent: int = 0
    is_header: bool = False


def _summary_rows(summary: TestSummary) -> list[_Row]:
    """Build the canonical metric rows shared by both renderers.

    Row kinds: normal rows carry a label and value; ``is_header`` rows are
    rich-only section headers (skipped by the plain renderer); rows with an
    empty label carry preformatted lines (histogram bars) rendered verbatim.
    """
    rows = [_Row("Target URL", summary.url)]
    if summary.timestamp:
        rows.append(_Row("Timestamp", summary.timestamp))
    rows.append(_Row("Duration", f"{summary.duration_secs:.1f}s"))
    rows.append(_Row("Workers", str(summary.workers)))

    rows.append(_Row("Total Requests", _format_number(summary.total_requests)))
    if summary.duration_secs > 0:
        rps = summary.total_requests / summary.duration_secs
        rows.append(_Row("Requests/sec", f"{rps:.1f}"))
    rows.append(_Row("Total Received", _format_bytes(summary.total_bytes_received)))

    rows.append(_Row("Min Latency", f"{summary.min_latency_ms:.2f} ms"))
    rows.append(_Row("Avg Latency", f"{summary.average_latency_ms:.2f} ms"))
    rows.append(_Row("P50 Latency", f"{summary.p50_latency_ms:.2f} ms"))
    rows.append(_Row("P90 Latency", f"{summary.p90_latency_ms:.2f} ms"))
    rows.append(_Row("P95 Latency", f"{summary.p95_latency_ms:.2f} ms"))
    rows.append(_Row("P99 Latency", f"{summary.p99_latency_ms:.2f} ms"))
    rows.append(_Row("Max Latency", f"{summary.max_latency_ms:.2f} ms"))
    rows.append(_Row("Std Dev (Jitter)", f"{summary.std_dev_latency_ms:.2f} ms"))
    rows.append(_Row("P99.99 Latency", f"{summary.p99_99_latency_ms:.2f} ms"))

    histogram_lines = _render_histogram(summary.latency_histogram)
    if histogram_lines:
        rows.append(_Row("Histogram", "", is_header=True))
        rows.extend(_Row("", line) for line in histogram_lines)

    if summary.total_errors > 0:
        rate = _error_rate(summary.total_requests, summary.total_errors)
        rows.append(
            _Row(
                "Errors",
                f"{_format_number(summary.total_errors)} ({rate})",
                style="error",
            )
        )
    else:
        rows.append(
            _Row(
                "Errors",
                f"{_format_number(summary.total_errors)} (0.00%)",
                style="success",
            )
        )
    rows.append(_Row("Status Codes", _format_status_codes(summary.status_codes)))

    if summary.avg_e2e_latency_us > 0.0:
        rows.append(
            _Row("Avg E2E Latency", f"{summary.avg_e2e_latency_us / 1000:.2f} ms")
        )

    if _is_grpc_mapped(summary.status_codes):
        rows.append(_Row("Protocol", "gRPC (status codes mapped to HTTP equivalents)"))

    if summary.chaos_injected_total > 0:
        rows.append(
            _Row(
                "Chaos Faults",
                f"{_format_number(summary.chaos_injected_total)} injected",
            )
        )
        for fault_type, count in summary.chaos_faults_by_type.items():
            rows.append(_Row(fault_type, _format_number(count), indent=1))

    sm = summary.system_metrics
    if isinstance(sm, SystemMetrics):
        mb = sm.peak_memory_rss_bytes / (1024 * 1024)
        rows.append(
            _Row(
                "Client Footprint",
                f"Peak CPU: {sm.peak_cpu_percent:.1f}% | "
                f"Peak RSS: {mb:.1f} MB | "
                f"Peak Threads: {sm.peak_thread_count}",
            )
        )

    ws = summary.ws
    if isinstance(ws, WebsocketMetrics):
        rows.append(
            _Row(
                "WS Heartbeat",
                f"Pings: {_format_number(ws.pings_sent_total)} sent / "
                f"{_format_number(ws.pings_received_total)} recv | "
                f"Pongs: {_format_number(ws.pongs_solicited_total)} solicited / "
                f"{_format_number(ws.pongs_unsolicited_total)} unsolicited",
            )
        )
        if ws.backpressure_max_bytes > 0 or ws.backpressure_threshold_breaches > 0:
            rows.append(
                _Row(
                    "WS Backpressure",
                    f"Max {_format_number(ws.backpressure_max_bytes)} B | "
                    f"Mean {ws.backpressure_mean_bytes:.0f} B | "
                    f"Breaches {_format_number(ws.backpressure_threshold_breaches)}",
                )
            )

    grpc = summary.grpc
    if isinstance(grpc, GrpcMetrics):
        rows.append(
            _Row(
                "gRPC Streams",
                f"Peak active {_format_number(grpc.active_streams_peak)} | "
                f"Util peak {grpc.concurrency_utilization_peak:.3f} / "
                f"mean {grpc.concurrency_utilization_mean:.3f}",
            )
        )
        if (
            grpc.window_exhaustion_events_total > 0
            or grpc.window_stall_duration_ms_total > 0
        ):
            rows.append(
                _Row(
                    "gRPC Window",
                    f"Exhaustions {_format_number(grpc.window_exhaustion_events_total)} | "
                    f"Stall {grpc.window_stall_duration_ms_total:.1f} ms | "
                    f"Min credit {_format_number(grpc.send_capacity_min_bytes)} B",
                )
            )

    http3 = summary.http3
    if isinstance(http3, Http3Metrics):
        rows.append(
            _Row(
                "HTTP/3 cwnd",
                f"Cur {_format_number(http3.cwnd_bytes_current)} B | "
                f"min {_format_number(http3.cwnd_bytes_min)} / "
                f"max {_format_number(http3.cwnd_bytes_max)} / "
                f"mean {http3.cwnd_bytes_mean:.0f} B",
            )
        )
        if http3.migrations_attempted_total > 0:
            rows.append(
                _Row(
                    "HTTP/3 Migration",
                    f"{_format_number(http3.migrations_attempted_total)} attempted / "
                    f"{_format_number(http3.migrations_successful_total)} successful "
                    f"({http3.migration_success_rate:.0%})",
                )
            )

    return rows


def _print_rich(
    summary: TestSummary,
) -> None:
    console = Console()

    table = Table(title="Load Test Results", show_lines=True, padding=(0, 1))
    table.add_column("Metric", style="bold cyan", no_wrap=True)
    table.add_column("Value", justify="right")

    for row in _summary_rows(summary):
        value = row.value
        if row.style == "error":
            value = f"[bold red]{value}[/]"
        elif row.style == "success":
            value = f"[green]{value}[/]"
        table.add_row(f"{'  ' * row.indent}{row.label}", value)

    console.print()
    console.print(table)
    console.print()


def _print_plain(
    summary: TestSummary,
) -> None:
    use_color = (
        not os.environ.get("NO_COLOR")
        and hasattr(sys.stdout, "isatty")
        and sys.stdout.isatty()
    )

    RED = "\033[91m" if use_color else ""
    GREEN = "\033[92m" if use_color else ""
    BOLD = "\033[1m" if use_color else ""
    RESET = "\033[0m" if use_color else ""

    width = 44
    sep = "=" * width

    lines = [f"{BOLD}{'Load Test Results':^{width}}{RESET}", sep]

    for row in _summary_rows(summary):
        if row.is_header:
            continue
        value = row.value
        if row.style == "error":
            value = f"{RED}{value}{RESET}"
        elif row.style == "success":
            value = f"{GREEN}{value}{RESET}"
        if not row.label:
            lines.append(value)
        elif row.indent:
            lines.append(f"{'  ' * (row.indent + 1)}{row.label}: {value}")
        else:
            lines.append(f"  {row.label + ':':<16} {value}")

    lines.append(sep)

    print("\n".join(lines))


def _metrics_description() -> None:
    print("Metric Descriptions:")
    print("- Min Latency: Minimum round-trip time across all completed requests.")
    print(
        "- Avg Latency: Mean round-trip time across all completed requests (lower is better)."
    )
    print("- P50 Latency: 50% of requests completed faster than this time (median).")
    print("- P90 Latency: 90% of requests completed faster than this time.")
    print(
        "- P95 Latency: 95% of requests completed faster than this time (tail latency, lower is better)."
    )
    print(
        "- P99 Latency: 99% of requests completed faster than this time (worst-case spikes)."
    )
    print("- Max Latency: Maximum round-trip time across all completed requests.")


def generate_markdown_summary(summary: TestSummary, config: object) -> str:
    """Generate a Markdown summary string from TestSummary + config.

    Returns a GitHub Actions / PR comment ready Markdown string with
    status badge, metrics table, and collapsible error details.
    """
    from strobengine.reporting.markdown_report import (
        generate_markdown_summary as _gen,
    )

    artifact = build_artifact_dict(summary, config)
    return _gen(artifact)


def generate_junit_report(summary: TestSummary, config: object) -> str:
    """Generate a JUnit XML string from TestSummary + config.

    Returns JUnit XML with performance assertion testcases for
    CI pipeline ingestion.
    """
    from strobengine.reporting.junit_report import (
        generate_junit_report as _gen,
    )

    artifact = build_artifact_dict(summary, config)
    return _gen(artifact)


def generate_csv_report(summary: TestSummary, config: object) -> str:
    """Generate a CSV string from TestSummary + config.

    Returns CSV with microsecond latencies for schema consistency.
    """
    from strobengine.reporting.csv_report import (
        generate_csv_report as _gen,
    )

    artifact = build_artifact_dict(summary, config)
    return _gen(artifact)
