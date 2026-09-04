from __future__ import annotations

import os
import sys

from strobengine._strobengine import TestSummary

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


def _is_grpc_mapped(codes: dict[int, int]) -> bool:
    """Check if status codes contain gRPC-mapped HTTP equivalents."""
    grpc_mapped_codes = {499, 504}  # Cancelled, DeadlineExceeded
    return any(code in grpc_mapped_codes for code in codes)


def _print_rich(
    summary: TestSummary,
) -> None:
    console = Console()

    table = Table(title="Load Test Results", show_lines=True, padding=(0, 1))
    table.add_column("Metric", style="bold cyan", no_wrap=True)
    table.add_column("Value", justify="right")

    # Execution context
    table.add_row("Target URL", summary.url)
    if summary.timestamp:
        table.add_row("Timestamp", summary.timestamp)
    table.add_row("Duration", f"{summary.duration_secs:.1f}s")
    table.add_row("Workers", str(summary.workers))

    # Throughput
    table.add_row("Total Requests", _format_number(summary.total_requests))
    if summary.duration_secs > 0:
        rps = summary.total_requests / summary.duration_secs
        table.add_row("Requests/sec", f"{rps:.1f}")
    table.add_row("Total Received", _format_bytes(summary.total_bytes_received))

    # Latency distribution
    table.add_row("Min Latency", f"{summary.min_latency_ms:.2f} ms")
    table.add_row("Avg Latency", f"{summary.average_latency_ms:.2f} ms")
    table.add_row("P50 Latency", f"{summary.p50_latency_ms:.2f} ms")
    table.add_row("P90 Latency", f"{summary.p90_latency_ms:.2f} ms")
    table.add_row("P95 Latency", f"{summary.p95_latency_ms:.2f} ms")
    table.add_row("P99 Latency", f"{summary.p99_latency_ms:.2f} ms")
    table.add_row("Max Latency", f"{summary.max_latency_ms:.2f} ms")

    # Errors
    if summary.total_errors > 0:
        rate = _error_rate(summary.total_requests, summary.total_errors)
        table.add_row(
            "Errors",
            f"[bold red]{_format_number(summary.total_errors)} ({rate})[/]",
        )
    else:
        table.add_row(
            "Errors",
            f"[green]{_format_number(summary.total_errors)} (0.00%)[/]",
        )
    table.add_row("Status Codes", _format_status_codes(summary.status_codes))

    # E2E latency (pub/sub only)
    if summary.avg_e2e_latency_us > 0.0:
        table.add_row(
            "Avg E2E Latency",
            f"{summary.avg_e2e_latency_us / 1000:.2f} ms",
        )

    # gRPC protocol note
    if _is_grpc_mapped(summary.status_codes):
        table.add_row("Protocol", "gRPC (status codes mapped to HTTP equivalents)")

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

    lines = [
        f"{BOLD}{'Load Test Results':^{width}}{RESET}",
        sep,
        f"  Target URL:     {summary.url}",
    ]

    if summary.timestamp:
        lines.append(f"  Timestamp:      {summary.timestamp}")
    lines.append(f"  Duration:       {summary.duration_secs:.1f}s")
    lines.append(f"  Workers:        {summary.workers}")

    lines.append(f"  Total Requests: {_format_number(summary.total_requests)}")
    if summary.duration_secs > 0:
        rps = summary.total_requests / summary.duration_secs
        lines.append(f"  Requests/sec:   {rps:.1f}")
    lines.append(f"  Total Received: {_format_bytes(summary.total_bytes_received)}")

    lines.append(f"  Min Latency:    {summary.min_latency_ms:.2f} ms")
    lines.append(f"  Avg Latency:    {summary.average_latency_ms:.2f} ms")
    lines.append(f"  P50 Latency:    {summary.p50_latency_ms:.2f} ms")
    lines.append(f"  P90 Latency:    {summary.p90_latency_ms:.2f} ms")
    lines.append(f"  P95 Latency:    {summary.p95_latency_ms:.2f} ms")
    lines.append(f"  P99 Latency:    {summary.p99_latency_ms:.2f} ms")
    lines.append(f"  Max Latency:    {summary.max_latency_ms:.2f} ms")

    if summary.total_errors > 0:
        rate = _error_rate(summary.total_requests, summary.total_errors)
        lines.append(
            f"  Errors:         {RED}{_format_number(summary.total_errors)} ({rate}){RESET}"
        )
    else:
        lines.append(
            f"  Errors:         {GREEN}{_format_number(summary.total_errors)} (0.00%){RESET}"
        )
    lines.append(f"  Status Codes:   {_format_status_codes(summary.status_codes)}")

    # E2E latency (pub/sub only)
    if summary.avg_e2e_latency_us > 0.0:
        lines.append(f"  Avg E2E Latency:{summary.avg_e2e_latency_us / 1000:.2f} ms")

    if _is_grpc_mapped(summary.status_codes):
        lines.append("  Protocol:       gRPC (status codes mapped to HTTP equivalents)")
    lines.append(sep)

    print("\n".join(lines))


def _metrics_description():
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


# ---------------------------------------------------------------------------
# Report Artifact Persistence
# ---------------------------------------------------------------------------

DEFAULT_REPORT_DIR = ".strobengine/reports"


def _slugify_url(url: str) -> str:
    """Convert URL to a safe filename slug."""
    import re

    return re.sub(r"[^a-zA-Z0-9_-]", "_", url)[:80]


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
    """Build a ReportArtifact dict matching the Rust schema in report/schema.rs."""
    successful = summary.total_requests - summary.total_errors
    rps = (
        summary.total_requests / summary.duration_secs
        if summary.duration_secs > 0
        else 0.0
    )

    # Extract CLI options from config (TestConfig PyO3 object)
    cli_options = {
        "method": getattr(config, "method", "GET"),
        "concurrency": getattr(config, "concurrency", 0),
        "timeout_secs": getattr(config, "timeout_secs", 0),
        "chaos": getattr(config, "chaos", False),
        "chaos_rate": getattr(config, "chaos_rate", 0.1),
        "body": getattr(config, "body", None),
        "headers": getattr(config, "headers", None),
    }

    # Get protocol-specific sections from summary.to_dict()
    summary_dict = summary.to_dict()

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
            "min_us": round(summary.min_latency_ms * 1000.0, 1),
            "max_us": round(summary.max_latency_ms * 1000.0, 1),
            "mean_us": round(summary.average_latency_ms * 1000.0, 1),
        },
        "error_breakdown": {str(k): v for k, v in summary.status_codes.items()},
        "avg_connection_latency_us": summary.avg_connection_latency_us,
        "quic": summary_dict.get("quic"),
        "sse": summary_dict.get("sse"),
    }


def save_report(
    summary: TestSummary,
    config: object,
    output_dir: str | None = None,
    no_save: bool = False,
) -> str | None:
    """Persist ReportArtifact to disk. Returns filepath or None."""
    if no_save:
        return None

    import json
    from pathlib import Path

    dirpath = Path(output_dir or DEFAULT_REPORT_DIR)
    dirpath.mkdir(parents=True, exist_ok=True)
    filename = f"{summary.timestamp}_{_slugify_url(summary.url)}.json"
    filepath = dirpath / filename
    artifact = build_artifact_dict(summary, config)
    filepath.write_text(json.dumps(artifact, indent=2))
    return str(filepath)


def generate_markdown_summary(summary, config) -> str:
    """Generate a Markdown summary string from TestSummary + config.

    Returns a GitHub Actions / PR comment ready Markdown string with
    status badge, metrics table, and collapsible error details.
    """
    from strobengine.reporting.markdown_report import (
        generate_markdown_summary as _gen,
    )

    artifact = build_artifact_dict(summary, config)
    return _gen(artifact)


def generate_junit_report(summary, config) -> str:
    """Generate a JUnit XML string from TestSummary + config.

    Returns JUnit XML with performance assertion testcases for
    CI pipeline ingestion.
    """
    from strobengine.reporting.junit_report import (
        generate_junit_report as _gen,
    )

    artifact = build_artifact_dict(summary, config)
    return _gen(artifact)


def generate_csv_report(summary, config) -> str:
    """Generate a CSV string from TestSummary + config.

    Returns CSV with microsecond latencies for schema consistency.
    """
    from strobengine.reporting.csv_report import (
        generate_csv_report as _gen,
    )

    artifact = build_artifact_dict(summary, config)
    return _gen(artifact)
