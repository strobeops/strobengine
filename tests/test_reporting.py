"""Unit tests for reporting modules (Markdown summary, export generators)."""

from unittest.mock import Mock


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
        from strobengine.reporting.junit_report import generate_junit_report
        from strobengine.reporter import build_artifact_dict

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
        from strobengine.reporting.junit_report import generate_junit_report
        from strobengine.reporter import build_artifact_dict

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
