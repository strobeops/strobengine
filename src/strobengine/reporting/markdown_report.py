"""Markdown report generator for CI/CD PR integration."""

from __future__ import annotations

from pathlib import Path

from strobengine.reporter import build_artifact_dict


def generate_markdown_summary(artifact: dict) -> str:
    """Generate a GitHub Actions / PR comment ready Markdown summary.

    Includes a status badge (PASS/FAIL), execution metadata, metrics table,
    and collapsible error details section when non-2xx responses exist.
    """
    meta = artifact["metadata"]
    s = artifact["summary"]
    lp = artifact["latency_percentiles"]
    errors = artifact["error_breakdown"]

    # Status badge — FAIL if any errors or zero requests
    total = s["total_requests"]
    failed = s["failed_requests"]
    error_rate = (failed / total * 100) if total > 0 else 0.0
    status = "PASS" if failed == 0 and total > 0 else "FAIL"
    badge_color = "brightgreen" if status == "PASS" else "red"

    lines = [
        f"# Load Test Results ![status](https://img.shields.io/badge/status-{status}-{badge_color})",
        "",
        "## Execution",
        "",
        f"- **Target URL:** `{meta['target_url']}`",
        f"- **Duration:** {meta['duration_secs']:.1f}s",
        f"- **Concurrency:** {meta['cli_options']['concurrency']}",
        f"- **Method:** {meta['cli_options']['method']}",
        "",
        "## Metrics",
        "",
        "| Metric | Value |",
        "|--------|-------|",
        f"| Total Requests | {total:,} |",
        f"| Successful | {s['successful_requests']:,} |",
        f"| Failed | {failed:,} |",
        f"| Requests/sec | {s['rps']:.2f} |",
        f"| P50 Latency | {lp['p50_us'] / 1000:.2f} ms |",
        f"| P90 Latency | {lp['p90_us'] / 1000:.2f} ms |",
        f"| P95 Latency | {lp['p95_us'] / 1000:.2f} ms |",
        f"| P99 Latency | {lp['p99_us'] / 1000:.2f} ms |",
        f"| Error Rate | {error_rate:.2f}% |",
        "",
    ]

    # Collapsible error details (only if non-2xx errors exist)
    error_codes = {
        code: count
        for code, count in errors.items()
        if str(code).isdigit() and int(code) >= 400
    }
    if error_codes:
        lines.extend(
            [
                "<details>",
                "<summary>Error Details</summary>",
                "",
                "| Status Code | Count |",
                "|-------------|-------|",
            ]
        )
        for code, count in sorted(
            error_codes.items(),
            key=lambda x: int(x[0]) if str(x[0]).isdigit() else 0,
        ):
            lines.append(f"| {code} | {count:,} |")
        lines.extend(["", "</details>", ""])

    return "\n".join(lines)


def render_markdown_report(summary, config, duration_secs: float) -> str:
    """Render a Markdown report from TestSummary (full format with all details)."""
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
            key=lambda x: int(x[0]) if str(x[0]).isdigit() else 0,
        ):
            lines.append(f"| {code} | {count:,} |")
        lines.append("")

    return "\n".join(lines)


def save_markdown_report(summary, config, filepath: str, duration_secs: float) -> str:
    """Render and write Markdown report to disk. Returns filepath."""
    filepath = str(Path(filepath).expanduser().resolve())
    artifact = build_artifact_dict(summary, config)
    md = generate_markdown_summary(artifact)
    Path(filepath).parent.mkdir(parents=True, exist_ok=True)
    Path(filepath).write_text(md)
    return filepath
