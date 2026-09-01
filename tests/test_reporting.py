"""Unit tests for reporting modules (Markdown summary, export generators)."""

from unittest.mock import Mock

import pytest
import typer


def _make_summary(**kwargs):
    """Create a mock TestSummary with sensible defaults."""
    summary = Mock()
    defaults = {
        "url": "http://localhost:8080",
        "total_requests": 100,
        "total_errors": 0,
        "average_latency_ms": 1.5,
        "p50_latency_ms": 1.0,
        "p90_latency_ms": 2.0,
        "p95_latency_ms": 3.0,
        "p99_latency_ms": 5.0,
        "min_latency_ms": 0.1,
        "max_latency_ms": 10.0,
        "total_bytes_received": 1024,
        "duration_secs": 10.0,
        "workers": 5,
        "timestamp": "2026-08-28T10:00:00Z",
        "raw_command": None,
        "status_codes": {200: 100},
        "avg_e2e_latency_us": 0.0,
    }
    defaults.update(kwargs)
    for k, v in defaults.items():
        setattr(summary, k, v)
    return summary


def _make_config(**kwargs):
    """Create a mock config with sensible defaults."""
    config = Mock()
    defaults = {
        "url": "http://localhost:8080",
        "method": "GET",
        "concurrency": 10,
        "timeout_secs": 10,
        "chaos": False,
        "chaos_rate": 0.1,
        "body": None,
        "headers": None,
    }
    defaults.update(kwargs)
    for k, v in defaults.items():
        setattr(config, k, v)
    return config


class TestMarkdownSummary:
    """Tests for generate_markdown_summary."""

    def test_pass_badge_when_no_errors(self):
        from strobengine.reporter import build_artifact_dict
        from strobengine.reporting.markdown_report import generate_markdown_summary

        artifact = build_artifact_dict(_make_summary(), _make_config())
        md = generate_markdown_summary(artifact)
        assert "PASS" in md
        assert "brightgreen" in md

    def test_fail_badge_when_errors(self):
        from strobengine.reporter import build_artifact_dict
        from strobengine.reporting.markdown_report import generate_markdown_summary

        artifact = build_artifact_dict(
            _make_summary(total_errors=5, status_codes={200: 95, 500: 5}),
            _make_config(),
        )
        md = generate_markdown_summary(artifact)
        assert "FAIL" in md
        assert "red" in md

    def test_has_metrics_table(self):
        from strobengine.reporter import build_artifact_dict
        from strobengine.reporting.markdown_report import generate_markdown_summary

        artifact = build_artifact_dict(_make_summary(), _make_config())
        md = generate_markdown_summary(artifact)
        assert "P95 Latency" in md
        assert "Requests/sec" in md
        assert "Total Requests" in md

    def test_error_details_collapsible(self):
        from strobengine.reporter import build_artifact_dict
        from strobengine.reporting.markdown_report import generate_markdown_summary

        artifact = build_artifact_dict(
            _make_summary(total_errors=5, status_codes={200: 95, 500: 5}),
            _make_config(),
        )
        md = generate_markdown_summary(artifact)
        assert "<details>" in md
        assert "500" in md
        assert "</details>" in md

    def test_no_errors_no_details_section(self):
        from strobengine.reporter import build_artifact_dict
        from strobengine.reporting.markdown_report import generate_markdown_summary

        artifact = build_artifact_dict(_make_summary(), _make_config())
        md = generate_markdown_summary(artifact)
        assert "<details>" not in md

    def test_zero_requests_no_crash(self):
        from strobengine.reporter import build_artifact_dict
        from strobengine.reporting.markdown_report import generate_markdown_summary

        artifact = build_artifact_dict(
            _make_summary(total_requests=0, total_errors=0, status_codes={}),
            _make_config(),
        )
        md = generate_markdown_summary(artifact)
        assert "FAIL" in md  # zero requests = FAIL
        assert "0.00%" in md


class TestMarkdownReportFile:
    """Tests for save_markdown_report file output."""

    def test_save_markdown_report(self, tmp_path):
        from strobengine.reporting.markdown_report import save_markdown_report

        filepath = str(tmp_path / "report.md")
        result = save_markdown_report(_make_summary(), _make_config(), filepath, 10.0)
        assert result == filepath
        assert (tmp_path / "report.md").exists()
        content = (tmp_path / "report.md").read_text()
        assert "PASS" in content

    def test_save_markdown_report_expands_user(self, tmp_path, monkeypatch):
        from strobengine.reporting.markdown_report import save_markdown_report

        monkeypatch.setenv("HOME", str(tmp_path))
        result = save_markdown_report(
            _make_summary(), _make_config(), "~/test_report.md", 10.0
        )
        assert result.startswith(str(tmp_path))


class TestJUnitReport:
    """Tests for generate_junit_report."""

    def test_generate_junit_xml_structure(self):
        from strobengine.reporter import build_artifact_dict
        from strobengine.reporting.junit_report import generate_junit_report

        artifact = build_artifact_dict(_make_summary(), _make_config())
        xml = generate_junit_report(artifact)
        assert "<testsuites>" in xml
        assert "<testsuite" in xml
        assert "<testcase" in xml

    def test_generate_junit_failure_when_errors(self):
        from strobengine.reporter import build_artifact_dict
        from strobengine.reporting.junit_report import generate_junit_report

        # 5 errors out of 100 = 5% > 1% threshold
        # Triggers failures on both load_test and error_rate_threshold testcases
        artifact = build_artifact_dict(
            _make_summary(total_errors=5, status_codes={200: 95, 500: 5}),
            _make_config(),
        )
        xml = generate_junit_report(artifact)
        assert 'failures="2"' in xml
        assert "performance_regression" in xml

    def test_generate_junit_no_failure_when_clean(self):
        from strobengine.reporter import build_artifact_dict
        from strobengine.reporting.junit_report import generate_junit_report

        artifact = build_artifact_dict(_make_summary(), _make_config())
        xml = generate_junit_report(artifact)
        assert 'failures="0"' in xml
        assert "<failure" not in xml

    def test_generate_junit_error_rate_threshold(self):
        from strobengine.reporter import build_artifact_dict
        from strobengine.reporting.junit_report import generate_junit_report

        # 5 errors out of 100 = 5% > 1% threshold
        artifact = build_artifact_dict(
            _make_summary(total_errors=5, status_codes={200: 95, 500: 5}),
            _make_config(),
        )
        xml = generate_junit_report(artifact)
        assert "error_rate_threshold" in xml
        assert "threshold_breach" in xml

    def test_generate_junit_p95_threshold(self):
        from strobengine.reporter import build_artifact_dict
        from strobengine.reporting.junit_report import generate_junit_report

        # P95 = 150ms > 100ms threshold
        artifact = build_artifact_dict(
            _make_summary(p95_latency_ms=150.0),
            _make_config(),
        )
        xml = generate_junit_report(artifact)
        assert "p95_latency_threshold" in xml
        assert "threshold_breach" in xml


class TestCSVReport:
    """Tests for generate_csv_report."""

    def test_generate_csv_has_header(self):
        from strobengine.reporter import build_artifact_dict
        from strobengine.reporting.csv_report import generate_csv_report

        artifact = build_artifact_dict(_make_summary(), _make_config())
        csv_output = generate_csv_report(artifact)
        first_line = csv_output.strip().split("\n")[0]
        assert first_line == "metric,value"

    def test_generate_csv_has_all_fields(self):
        from strobengine.reporter import build_artifact_dict
        from strobengine.reporting.csv_report import generate_csv_report

        artifact = build_artifact_dict(_make_summary(), _make_config())
        csv_output = generate_csv_report(artifact)
        assert "timestamp" in csv_output
        assert "target_url" in csv_output
        assert "total_requests" in csv_output
        assert "rps" in csv_output
        assert "p50_us" in csv_output
        assert "p95_us" in csv_output
        assert "p99_us" in csv_output

    def test_generate_csv_microsecond_latencies(self):
        from strobengine.reporter import build_artifact_dict
        from strobengine.reporting.csv_report import generate_csv_report

        artifact = build_artifact_dict(_make_summary(), _make_config())
        csv_output = generate_csv_report(artifact)
        # p50 = 1.0 ms = 1000 us
        assert "p50_us,1000" in csv_output
        # p95 = 3.0 ms = 3000 us
        assert "p95_us,3000" in csv_output

    def test_generate_csv_file_output(self, tmp_path):
        from strobengine.reporting.csv_report import save_csv_report

        filepath = str(tmp_path / "report.csv")
        result = save_csv_report(_make_summary(), _make_config(), filepath, 10.0)
        assert result == filepath
        assert (tmp_path / "report.csv").exists()
        content = (tmp_path / "report.csv").read_text()
        assert "metric,value" in content


class TestBaselineArtifact:
    """Tests for load_baseline_artifact."""

    def test_load_baseline_from_file(self, tmp_path):
        from strobengine.reporting.baseline import load_baseline_artifact

        artifact = {
            "metadata": {
                "timestamp": "2026-01-01T00:00:00Z",
                "target_url": "http://test",
            },
            "summary": {"rps": 10.0, "total_requests": 100, "failed_requests": 0},
            "latency_percentiles": {"p95_us": 5000.0},
        }
        filepath = tmp_path / "report.json"
        filepath.write_text(__import__("json").dumps(artifact))

        result = load_baseline_artifact(baseline_file=filepath)
        assert result is not None
        assert result["metadata"]["target_url"] == "http://test"

    def test_load_baseline_file_not_found(self, tmp_path):
        from strobengine.reporting.baseline import load_baseline_artifact

        result = load_baseline_artifact(baseline_file=tmp_path / "nonexistent.json")
        assert result is None

    def test_load_baseline_corrupt_json(self, tmp_path):
        from strobengine.reporting.baseline import load_baseline_artifact

        filepath = tmp_path / "corrupt.json"
        filepath.write_text("not valid json {{{")

        result = load_baseline_artifact(baseline_file=filepath)
        assert result is None

    def test_load_baseline_latest_pointer(self, tmp_path):
        from strobengine.reporting.baseline import load_baseline_artifact

        artifact = {
            "metadata": {
                "timestamp": "2026-01-01T00:00:00Z",
                "target_url": "http://test",
            },
            "summary": {"rps": 10.0, "total_requests": 100, "failed_requests": 0},
            "latency_percentiles": {"p95_us": 5000.0},
        }
        report_file = tmp_path / "report_20260101_000000_test.json"
        report_file.write_text(__import__("json").dumps(artifact))

        latest = tmp_path / "latest.json"
        latest.write_text(__import__("json").dumps({"latest_report": report_file.name}))

        result = load_baseline_artifact(report_dir=tmp_path)
        assert result is not None
        assert result["summary"]["rps"] == 10.0

    def test_load_baseline_latest_missing_target(self, tmp_path):
        from strobengine.reporting.baseline import load_baseline_artifact

        latest = tmp_path / "latest.json"
        latest.write_text(
            __import__("json").dumps({"latest_report": "nonexistent.json"})
        )

        result = load_baseline_artifact(report_dir=tmp_path)
        assert result is None

    def test_load_baseline_no_latest_file(self, tmp_path):
        from strobengine.reporting.baseline import load_baseline_artifact

        result = load_baseline_artifact(report_dir=tmp_path)
        assert result is None


class TestComputeComparison:
    """Tests for compute_comparison."""

    def _make_artifact(self, rps, total_requests, failed_requests, p95_us):
        return {
            "metadata": {
                "timestamp": "2026-01-01T00:00:00Z",
                "target_url": "http://test",
            },
            "summary": {
                "total_requests": total_requests,
                "failed_requests": failed_requests,
                "rps": rps,
            },
            "latency_percentiles": {"p95_us": p95_us},
        }

    def test_compute_comparison_normal(self):
        from strobengine.reporting.baseline import compute_comparison

        current = self._make_artifact(
            rps=40.0, total_requests=200, failed_requests=10, p95_us=25000.0
        )
        baseline = self._make_artifact(
            rps=30.0, total_requests=150, failed_requests=5, p95_us=35000.0
        )

        result = compute_comparison(current, baseline)
        assert result["rps_delta"] == 33.33
        assert result["latency_p95_delta"] < 0  # improvement (lower latency)
        assert result["error_rate_delta"] > 0  # regression (more errors)

    def test_compute_comparison_improvement(self):
        from strobengine.reporting.baseline import compute_comparison

        current = self._make_artifact(
            rps=50.0, total_requests=500, failed_requests=0, p95_us=20000.0
        )
        baseline = self._make_artifact(
            rps=30.0, total_requests=300, failed_requests=30, p95_us=35000.0
        )

        result = compute_comparison(current, baseline)
        assert result["rps_delta"] > 0  # RPS improved
        assert result["latency_p95_delta"] < 0  # latency improved
        assert result["error_rate_delta"] < 0  # errors improved

    def test_compute_comparison_zero_baseline_rps(self):
        from strobengine.reporting.baseline import compute_comparison

        current = self._make_artifact(
            rps=10.0, total_requests=100, failed_requests=0, p95_us=5000.0
        )
        baseline = self._make_artifact(
            rps=0.0, total_requests=100, failed_requests=0, p95_us=5000.0
        )

        result = compute_comparison(current, baseline)
        assert result["rps_delta"] == 100.0  # base=0, curr>0 -> 100%

    def test_compute_comparison_zero_baseline_p95(self):
        from strobengine.reporting.baseline import compute_comparison

        current = self._make_artifact(
            rps=10.0, total_requests=100, failed_requests=0, p95_us=10000.0
        )
        baseline = self._make_artifact(
            rps=10.0, total_requests=100, failed_requests=0, p95_us=0.0
        )

        result = compute_comparison(current, baseline)
        assert result["latency_p95_delta"] == 100.0  # base=0, curr>0 -> 100%


class TestHTMLReport:
    """Tests for render_html_report and save_html_report."""

    def test_render_html_contains_charts(self):
        from strobengine.reporting.html_report import render_html_report

        html = render_html_report(_make_summary(), _make_config())
        assert "<canvas" in html
        assert "Chart" in html

    def test_render_html_contains_metadata(self):
        from strobengine.reporting.html_report import render_html_report

        html = render_html_report(_make_summary(), _make_config())
        assert "http://localhost:8080" in html
        assert "10.0s" in html

    def test_render_html_with_comparison(self):
        from strobengine.reporting.html_report import render_html_report

        comparison = {
            "baseline_timestamp": "2026-01-01T00:00:00Z",
            "rps_delta": 10.5,
            "latency_p95_delta": -5.2,
            "error_rate_delta": -1.0,
            "baseline_rps": 30.0,
            "baseline_p95_ms": 35.0,
            "baseline_error_rate": 5.0,
        }
        html = render_html_report(
            _make_summary(), _make_config(), comparison=comparison
        )
        assert "Historical Comparison" in html

    def test_render_html_without_comparison(self):
        from strobengine.reporting.html_report import render_html_report

        html = render_html_report(_make_summary(), _make_config(), comparison=None)
        assert "Historical Comparison" not in html

    def test_render_html_status_grouping(self):
        from strobengine.reporting.html_report import render_html_report

        summary = _make_summary(total_errors=10, status_codes={200: 90, 404: 5, 500: 5})
        html = render_html_report(summary, _make_config())
        assert "2xx" in html
        assert "4xx" in html
        assert "5xx" in html

    def test_save_html_report_file_output(self, tmp_path):
        from strobengine.reporting.html_report import save_html_report

        filepath = str(tmp_path / "report.html")
        result = save_html_report(_make_summary(), _make_config(), filepath)
        assert result == filepath
        assert (tmp_path / "report.html").exists()
        content = (tmp_path / "report.html").read_text()
        assert "<html" in content


class TestCLIHelpers:
    """Tests for cli.py helper functions."""

    def test_parse_headers_valid(self):
        from strobengine.cli import _parse_headers

        result = _parse_headers(["X-Custom: value"])
        assert result == [("X-Custom", "value")]

    def test_parse_headers_invalid(self):
        from strobengine.cli import _parse_headers

        with pytest.raises(typer.BadParameter):
            _parse_headers(["NoColonHere"])

    def test_validate_method_uppercase(self):
        from strobengine.cli import _validate_method

        assert _validate_method("get") == "GET"
        assert _validate_method("post") == "POST"

    def test_validate_method_invalid(self):
        from strobengine.cli import _validate_method

        with pytest.raises(typer.BadParameter):
            _validate_method("INVALID")
