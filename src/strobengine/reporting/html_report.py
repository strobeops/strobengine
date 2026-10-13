"""Standalone HTML report generator with Chart.js latency and status visualizations."""

from __future__ import annotations

from pathlib import Path

from jinja2 import Environment, FileSystemLoader

from strobengine._strobengine import TestSummary
from strobengine.artifact import build_artifact_dict
from strobengine.report_schema import SupportsCliOptions
from strobengine.reporting.baseline import ComparisonDict

# Load Chart.js from local asset for 100% offline reports
_ASSETS_DIR = Path(__file__).parent / "assets"
_CHART_JS_SOURCE = (_ASSETS_DIR / "chart.min.js").read_text(encoding="utf-8")

# Autoescape on: report values (target URL, baseline timestamps) may be
# hostile; trusted JS is explicitly marked with | safe in the template.
# Pre-compiled at module level for performance
_HTML_TEMPLATE = Environment(
    loader=FileSystemLoader(_ASSETS_DIR), autoescape=True
).get_template("report_template.html")


def render_html_report(
    summary: TestSummary,
    config: SupportsCliOptions,
    comparison: ComparisonDict | None = None,
) -> str:
    """Render a self-contained HTML report from TestSummary + config."""
    artifact = build_artifact_dict(summary, config)

    # Convert us -> ms for chart display
    from strobengine.reporting import us_to_ms

    lp = artifact["latency_percentiles"]
    latency_ms = {
        "p50": us_to_ms(lp.get("p50_us")),
        "p90": us_to_ms(lp.get("p90_us")),
        "p95": us_to_ms(lp.get("p95_us")),
        "p99": us_to_ms(lp.get("p99_us")),
    }
    # Full statistical breakdown for the deep-dive table.
    latency_stats = {
        "min": us_to_ms(lp.get("min_us")),
        "mean": us_to_ms(lp.get("mean_us")),
        "p50": latency_ms["p50"],
        "p90": latency_ms["p90"],
        "p95": latency_ms["p95"],
        "p99": latency_ms["p99"],
        "p99_99": us_to_ms(lp.get("p99_99_us")),
        "max": us_to_ms(lp.get("max_us")),
        "std_dev": us_to_ms(lp.get("std_dev_us")),
    }

    # Group status codes by class
    status_groups = {"2xx": 0, "4xx": 0, "5xx": 0, "other": 0}
    for code_str, count in artifact["error_breakdown"].items():
        code = int(code_str) if code_str.isdigit() else 0
        if 200 <= code < 300:
            status_groups["2xx"] += count
        elif 400 <= code < 500:
            status_groups["4xx"] += count
        elif 500 <= code < 600:
            status_groups["5xx"] += count
        else:
            status_groups["other"] += count

    # Failure classification for the error-breakdown card. A status of 0 is a
    # transport-level failure (timeout, reset, DNS); 4xx/5xx are HTTP failures.
    error_classes = {"transport": 0, "4xx": 0, "5xx": 0, "other": 0}
    error_codes = []
    for code_str, count in artifact["error_breakdown"].items():
        code = int(code_str) if code_str.isdigit() else 0
        if code == 0:
            error_classes["transport"] += count
            error_codes.append((code_str, count))
        elif 400 <= code < 500:
            error_classes["4xx"] += count
            error_codes.append((code_str, count))
        elif 500 <= code < 600:
            error_classes["5xx"] += count
            error_codes.append((code_str, count))
        elif code >= 400:
            error_classes["other"] += count
            error_codes.append((code_str, count))
    error_codes.sort(key=lambda x: int(x[0]) if x[0].isdigit() else 0)

    # Sort status codes for display
    status_codes = sorted(
        artifact["error_breakdown"].items(),
        key=lambda x: int(x[0]) if x[0].isdigit() else 0,
    )

    system_metrics = artifact.get("system_metrics")
    websocket = artifact.get("websocket")
    grpc = artifact.get("grpc")
    http3 = artifact.get("http3")
    slo = artifact.get("slo")

    return _HTML_TEMPLATE.render(
        metadata=artifact["metadata"],
        summary=artifact["summary"],
        latency=latency_ms,
        latency_stats=latency_stats,
        status_groups=status_groups,
        status_codes=status_codes,
        error_classes=error_classes,
        error_codes=error_codes,
        comparison=comparison,
        slo=slo,
        system_metrics=system_metrics,
        websocket=websocket,
        grpc=grpc,
        http3=http3,
        chart_js_code=_CHART_JS_SOURCE,
    )


def save_html_report(
    summary: TestSummary,
    config: SupportsCliOptions,
    filepath: str,
    comparison: ComparisonDict | None = None,
) -> str:
    """Render and write HTML report to disk. Returns filepath."""
    html = render_html_report(summary, config, comparison=comparison)
    Path(filepath).parent.mkdir(parents=True, exist_ok=True)
    Path(filepath).write_text(html)
    return filepath
