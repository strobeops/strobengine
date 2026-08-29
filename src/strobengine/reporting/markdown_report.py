"""Markdown report generator for CI/CD PR integration."""

from __future__ import annotations

from pathlib import Path

from strobengine.reporter import build_artifact_dict


def render_markdown_report(summary, config, duration_secs: float) -> str:
    """Render a Markdown report from TestSummary."""
    artifact = build_artifact_dict(summary, config)
    meta = artifact["metadata"]
    s = artifact["summary"]
    lp = artifact["latency_percentiles"]

    duration = duration_secs or meta.get("duration_secs", 0)
    error_rate = (
        (s["failed_requests"] / s["total_requests"] * 100)
        if s["total_requests"] > 0
        else 0.0
    )
    bytes_kb = s["bytes_transferred"] / 1024

    lines = [
        "# Load Test Results",
        "",
        "| Metric | Value |",
        "|--------|-------|",
        f"| Target URL | `{meta['target_url']}` |",
        f"| Timestamp | {meta['timestamp']} |",
        f"| Duration | {duration:.1f}s |",
        f"| Concurrency | {meta['cli_options']['concurrency']} |",
        f"| Method | {meta['cli_options']['method']} |",
        f"| Total Requests | {s['total_requests']:,} |",
        f"| Successful | {s['successful_requests']:,} |",
        f"| Failed | {s['failed_requests']:,} |",
        f"| Requests/sec | {s['rps']:.2f} |",
        f"| P50 Latency | {lp['p50_us'] / 1000:.2f} ms |",
        f"| P90 Latency | {lp['p90_us'] / 1000:.2f} ms |",
        f"| P95 Latency | {lp['p95_us'] / 1000:.2f} ms |",
        f"| P99 Latency | {lp['p99_us'] / 1000:.2f} ms |",
        f"| Min Latency | {lp['min_us'] / 1000:.2f} ms |",
        f"| Max Latency | {lp['max_us'] / 1000:.2f} ms |",
        f"| Mean Latency | {lp['mean_us'] / 1000:.2f} ms |",
        f"| Error Rate | {error_rate:.2f}% |",
        f"| Bytes Transferred | {bytes_kb:.1f} KB |",
        "",
    ]

    # Status code breakdown
    if artifact["error_breakdown"]:
        lines.append("## Status Codes")
        lines.append("")
        lines.append("| Code | Count |")
        lines.append("|------|-------|")
        for code, count in sorted(
            artifact["error_breakdown"].items(),
            key=lambda x: int(x[0]) if x[0].isdigit() else 0,
        ):
            lines.append(f"| {code} | {count:,} |")
        lines.append("")

    return "\n".join(lines)


def save_markdown_report(summary, config, filepath: str, duration_secs: float) -> str:
    """Render and write Markdown report to disk. Returns filepath."""
    filepath = str(Path(filepath).expanduser().resolve())
    md = render_markdown_report(summary, config, duration_secs)
    Path(filepath).parent.mkdir(parents=True, exist_ok=True)
    Path(filepath).write_text(md)
    return filepath
