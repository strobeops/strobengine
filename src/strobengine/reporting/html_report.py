"""Standalone HTML report generator with Chart.js latency and status visualizations."""

from __future__ import annotations

from pathlib import Path

from jinja2 import Template

from strobengine.reporter import build_artifact_dict

# Load Chart.js from local asset for 100% offline reports
_ASSETS_DIR = Path(__file__).parent / "assets"
_CHART_JS_SOURCE = (_ASSETS_DIR / "chart.min.js").read_text(encoding="utf-8")

# Pre-compiled at module level for performance
_HTML_TEMPLATE = Template(
    (_ASSETS_DIR / "report_template.html").read_text(encoding="utf-8")
)


def render_html_report(summary, config, comparison=None) -> str:
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

    # Sort status codes for display
    status_codes = sorted(
        artifact["error_breakdown"].items(),
        key=lambda x: int(x[0]) if x[0].isdigit() else 0,
    )

    return _HTML_TEMPLATE.render(
        metadata=artifact["metadata"],
        summary=artifact["summary"],
        latency=latency_ms,
        status_groups=status_groups,
        status_codes=status_codes,
        comparison=comparison,
        chart_js_code=_CHART_JS_SOURCE,
    )


def save_html_report(summary, config, filepath: str, comparison=None) -> str:
    """Render and write HTML report to disk. Returns filepath."""
    html = render_html_report(summary, config, comparison=comparison)
    Path(filepath).parent.mkdir(parents=True, exist_ok=True)
    Path(filepath).write_text(html)
    return filepath
