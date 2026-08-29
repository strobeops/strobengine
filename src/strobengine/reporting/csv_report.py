"""CSV report generator for data analysis and spreadsheet import."""

from __future__ import annotations

import csv
import io
from pathlib import Path

from strobengine.reporter import build_artifact_dict


def render_csv_report(summary, config, duration_secs: float) -> str:
    """Render a CSV report from TestSummary."""
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

    rows = [
        ("target_url", meta["target_url"]),
        ("timestamp", meta["timestamp"]),
        ("duration_secs", f"{duration:.1f}"),
        ("concurrency", str(meta["cli_options"]["concurrency"])),
        ("method", meta["cli_options"]["method"]),
        ("total_requests", str(s["total_requests"])),
        ("successful_requests", str(s["successful_requests"])),
        ("failed_requests", str(s["failed_requests"])),
        ("rps", f"{s['rps']:.2f}"),
        ("p50_ms", f"{lp['p50_us'] / 1000:.2f}"),
        ("p90_ms", f"{lp['p90_us'] / 1000:.2f}"),
        ("p95_ms", f"{lp['p95_us'] / 1000:.2f}"),
        ("p99_ms", f"{lp['p99_us'] / 1000:.2f}"),
        ("min_ms", f"{lp['min_us'] / 1000:.2f}"),
        ("max_ms", f"{lp['max_us'] / 1000:.2f}"),
        ("mean_ms", f"{lp['mean_us'] / 1000:.2f}"),
        ("error_rate_pct", f"{error_rate:.2f}"),
        ("bytes_transferred", str(s["bytes_transferred"])),
    ]

    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(["metric", "value"])
    writer.writerows(rows)
    return buf.getvalue()


def save_csv_report(summary, config, filepath: str, duration_secs: float) -> str:
    """Render and write CSV report to disk. Returns filepath."""
    filepath = str(Path(filepath).expanduser().resolve())
    csv_content = render_csv_report(summary, config, duration_secs)
    Path(filepath).parent.mkdir(parents=True, exist_ok=True)
    Path(filepath).write_text(csv_content)
    return filepath
