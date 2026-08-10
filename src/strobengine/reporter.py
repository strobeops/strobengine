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
    if n < 1024:
        return f"{n} B"
    if n < 1024 * 1024:
        return f"{n / 1024:.1f} KB"
    return f"{n / (1024 * 1024):.1f} MB"


def _error_rate(total: int, errors: int) -> str:
    if total == 0:
        return "0.00%"
    return f"{errors / total * 100:.2f}%"


def _format_status_codes(codes: dict[int, int]) -> str:
    if not codes:
        return "none"
    parts = [f"{code}: {count}" for code, count in sorted(codes.items())]
    return " | ".join(parts)


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
