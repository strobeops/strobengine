"""Baseline artifact loading and delta comparison for historical reporting."""

from __future__ import annotations

import json
import sys
from pathlib import Path


def load_baseline_artifact(
    report_dir: Path = Path(".strobengine/reports"),
    baseline_file: Path | None = None,
) -> dict | None:
    """Load baseline artifact from explicit file or latest.json pointer.

    Returns None if no baseline is available or file is corrupt.
    """
    try:
        if baseline_file:
            if not baseline_file.exists():
                print(
                    f"[warning] Baseline file not found: {baseline_file}",
                    file=sys.stderr,
                )
                return None
            return json.loads(baseline_file.read_text())

        # Try latest.json pointer
        latest_path = report_dir / "latest.json"
        if latest_path.exists():
            pointer = json.loads(latest_path.read_text())
            report_path = report_dir / pointer["latest_report"]
            if report_path.exists():
                return json.loads(report_path.read_text())

        return None
    except (json.JSONDecodeError, KeyError, OSError) as e:
        print(f"[warning] Could not load baseline: {e}", file=sys.stderr)
        return None


def compute_comparison(current: dict, baseline: dict) -> dict:
    """Compute delta metrics between current and baseline runs.

    Returns a dict with baseline metadata and percentage/point deltas
    for RPS, P95 latency, and error rate.
    """

    def pct_delta(curr_val: float, base_val: float) -> float:
        if base_val == 0:
            return 0.0 if curr_val == 0 else 100.0
        return round(((curr_val - base_val) / base_val) * 100, 2)

    curr_summary = current["summary"]
    base_summary = baseline["summary"]
    curr_lp = current["latency_percentiles"]
    base_lp = baseline["latency_percentiles"]

    curr_error_rate = (
        (curr_summary["failed_requests"] / curr_summary["total_requests"] * 100)
        if curr_summary["total_requests"] > 0
        else 0.0
    )
    base_error_rate = (
        (base_summary["failed_requests"] / base_summary["total_requests"] * 100)
        if base_summary["total_requests"] > 0
        else 0.0
    )

    return {
        "baseline_timestamp": baseline["metadata"]["timestamp"],
        "baseline_url": baseline["metadata"]["target_url"],
        "latency_p95_delta": pct_delta(curr_lp["p95_us"], base_lp["p95_us"]),
        "rps_delta": pct_delta(curr_summary["rps"], base_summary["rps"]),
        "error_rate_delta": round(curr_error_rate - base_error_rate, 2),
        "baseline_rps": base_summary["rps"],
        "baseline_p95_ms": round(base_lp["p95_us"] / 1000, 2),
        "baseline_error_rate": round(base_error_rate, 2),
    }
