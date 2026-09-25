"""Report output and export pipeline shared by the load/stress/spike commands.

Extracted from ``cli.py`` so the terminal printing, baseline comparison, and
HTML/Markdown/JUnit/CSV generation live in one focused module. Import of the
heavy `reporting.*` generators stays lazy (inside the functions) so the base
CLI import remains light.
"""

from __future__ import annotations

import sys
from dataclasses import dataclass

from strobengine._strobengine import TestSummary
from strobengine.reporter import print_summary


@dataclass
class ExportOptions:
    """Options controlling report output and persistence."""

    output_dir: str | None = None
    no_save: bool = False
    html_output: str | None = None
    compare_to: str | None = None
    export_markdown: str | None = None
    export_junit: str | None = None
    export_csv: str | None = None
    json_output: bool = False


def _report_saved(filepath: str | None, exports: ExportOptions) -> None:
    """Print report path to stderr unless in JSON mode or saving is disabled."""
    if filepath and not exports.json_output and not exports.no_save:
        print(f"Report saved to {filepath}", file=sys.stderr)


def _output_results(
    summary: TestSummary,
    url: str,
    duration_secs: int,
    config: object | None,
    exports: ExportOptions,
    saved_report_path: str | None = None,
) -> None:
    print_summary(summary, json_output=exports.json_output)
    _report_saved(saved_report_path, exports)

    # Compute comparison (runs for both --html and terminal display)
    comparison = None
    if exports.compare_to:
        from pathlib import Path

        from strobengine.reporter import build_artifact_dict
        from strobengine.reporting.baseline import (
            compute_comparison,
            load_baseline_artifact,
        )

        baseline = load_baseline_artifact(baseline_file=Path(exports.compare_to))
        if baseline and config is not None:
            current = build_artifact_dict(summary, config)
            comparison = compute_comparison(current, baseline)
            if not exports.json_output:
                from strobengine.reporting.baseline import print_cli_comparison

                print_cli_comparison(comparison)

    if exports.html_output:
        from strobengine.reporting.html_report import save_html_report

        save_html_report(summary, config, exports.html_output, comparison=comparison)
        _report_saved(exports.html_output, exports)

    # Export formats
    if exports.export_markdown:
        from strobengine.reporting.markdown_report import save_markdown_report

        save_markdown_report(summary, config, exports.export_markdown, duration_secs)
        _report_saved(exports.export_markdown, exports)

    if exports.export_junit:
        from strobengine.reporting.junit_report import save_junit_report

        save_junit_report(summary, config, exports.export_junit, duration_secs)
        _report_saved(exports.export_junit, exports)

    if exports.export_csv:
        from strobengine.reporting.csv_report import save_csv_report

        save_csv_report(summary, config, exports.export_csv, duration_secs)
        _report_saved(exports.export_csv, exports)
