"""CSV report generator for data analysis and spreadsheet import."""

from __future__ import annotations

import csv
import io
from pathlib import Path

from strobengine.reporter import build_artifact_dict


def generate_csv_report(artifact: dict) -> str:
    """Generate CSV with schema-consistent microsecond latencies.

    Output columns use `_us` suffix for latency fields to match
    the ReportArtifact JSON schema.
    """
    meta = artifact["metadata"]
    s = artifact["summary"]
    lp = artifact["latency_percentiles"]

    rows = [
        ("timestamp", meta["timestamp"]),
        ("target_url", meta["target_url"]),
        ("duration_s", f"{meta['duration_secs']:.1f}"),
        ("concurrency", str(meta["cli_options"]["concurrency"])),
        ("total_requests", str(s["total_requests"])),
        ("failed_requests", str(s["failed_requests"])),
        ("rps", f"{s['rps']:.2f}"),
        ("p50_us", str(int(lp["p50_us"]))),
        ("p90_us", str(int(lp["p90_us"]))),
        ("p95_us", str(int(lp["p95_us"]))),
        ("p99_us", str(int(lp["p99_us"]))),
    ]

    buf = io.StringIO()
    writer = csv.writer(buf, lineterminator="\n")
    writer.writerow(["metric", "value"])
    writer.writerows(rows)
    return buf.getvalue()


def render_csv_report(summary, config, duration_secs: float) -> str:
    """Render a CSV report from TestSummary (convenience wrapper)."""
    artifact = build_artifact_dict(summary, config)
    return generate_csv_report(artifact)


def save_csv_report(summary, config, filepath: str, duration_secs: float) -> str:
    """Render and write CSV report to disk. Returns filepath."""
    filepath = str(Path(filepath).expanduser().resolve())
    artifact = build_artifact_dict(summary, config)
    csv_content = generate_csv_report(artifact)
    Path(filepath).parent.mkdir(parents=True, exist_ok=True)
    Path(filepath).write_text(csv_content)
    return filepath
