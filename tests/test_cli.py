import json
from pathlib import Path
from unittest import mock

import pytest
from typer.testing import CliRunner

from strobengine.cli import app, main

runner = CliRunner()


class TestCLILoadSubcommand:
    def test_load_default(self, local_server: str) -> None:
        result = runner.invoke(app, ["load", local_server, "-c", "2", "-d", "1"])
        assert result.exit_code == 0

    def test_load_custom_flags(self, local_server: str) -> None:
        result = runner.invoke(
            app, ["load", local_server, "-c", "5", "-d", "2", "-t", "3"]
        )
        assert result.exit_code == 0


class TestCLIStressSubcommand:
    def test_stress_default(self, local_server: str) -> None:
        result = runner.invoke(
            app,
            [
                "stress",
                local_server,
                "--from",
                "2",
                "--to",
                "5",
                "--ramp",
                "1",
                "--hold",
                "1",
            ],
        )
        assert result.exit_code == 0

    def test_stress_custom_flags(self, local_server: str) -> None:
        result = runner.invoke(
            app,
            [
                "stress",
                local_server,
                "--from",
                "3",
                "--to",
                "10",
                "--ramp",
                "2",
                "--hold",
                "1",
                "-t",
                "3",
            ],
        )
        assert result.exit_code == 0


class TestCLISpikeSubcommand:
    def test_spike_default(self, local_server: str) -> None:
        result = runner.invoke(
            app,
            [
                "spike",
                local_server,
                "--baseline",
                "1",
                "--peak",
                "3",
                "--spike-duration",
                "1",
            ],
        )
        assert result.exit_code == 0

    def test_spike_custom_flags(self, local_server: str) -> None:
        result = runner.invoke(
            app,
            [
                "spike",
                local_server,
                "--baseline",
                "2",
                "--peak",
                "5",
                "--pre-spike",
                "1",
                "--spike-duration",
                "2",
                "--post-spike",
                "1",
                "-t",
                "3",
            ],
        )
        assert result.exit_code == 0


class TestCLIValidation:
    def test_load_invalid_concurrency(self) -> None:
        result = runner.invoke(app, ["load", "http://unused", "-c", "0"])
        assert result.exit_code != 0

    def test_load_invalid_duration(self) -> None:
        result = runner.invoke(app, ["load", "http://unused", "-d", "0"])
        assert result.exit_code != 0

    def test_stress_invalid_from(self) -> None:
        result = runner.invoke(app, ["stress", "http://unused", "--from", "0"])
        assert result.exit_code != 0

    def test_stress_invalid_to(self) -> None:
        result = runner.invoke(app, ["stress", "http://unused", "--to", "0"])
        assert result.exit_code != 0

    def test_spike_invalid_baseline(self) -> None:
        result = runner.invoke(app, ["spike", "http://unused", "--baseline", "0"])
        assert result.exit_code != 0

    def test_spike_invalid_peak(self) -> None:
        result = runner.invoke(app, ["spike", "http://unused", "--peak", "0"])
        assert result.exit_code != 0

    def test_load_concurrency_above_limit(self) -> None:
        result = runner.invoke(
            app, ["load", "http://unused", "-c", "10001", "-d", "1", "--no-save"]
        )
        assert result.exit_code != 0

    def test_stress_concurrency_above_limit(self) -> None:
        result = runner.invoke(
            app,
            [
                "stress",
                "http://unused",
                "--to",
                "10001",
                "--ramp",
                "1",
                "--hold",
                "1",
                "--no-save",
            ],
        )
        assert result.exit_code != 0

    def test_spike_concurrency_above_limit(self) -> None:
        result = runner.invoke(
            app,
            [
                "spike",
                "http://unused",
                "--peak",
                "10001",
                "--pre-spike",
                "0",
                "--spike-duration",
                "1",
                "--post-spike",
                "0",
                "--no-save",
            ],
        )
        assert result.exit_code != 0


class TestCompareToBaseline:
    """Malformed --compare-to baselines must warn and still write exports."""

    @pytest.mark.parametrize(
        "baseline_content",
        [
            pytest.param(
                {
                    "metadata": {},
                    "summary": {
                        "rps": 1.0,
                        "total_requests": 10,
                        "failed_requests": 0,
                    },
                    "latency_percentiles": {"p95_us": 1000.0},
                },
                id="missing-metadata-subkey",
            ),
            pytest.param(
                {
                    "metadata": {
                        "timestamp": "2026-01-01T00:00:00Z",
                        "target_url": "http://unused",
                    },
                },
                id="missing-summary",
            ),
            pytest.param(
                {
                    "metadata": {
                        "timestamp": "2026-01-01T00:00:00Z",
                        "target_url": "http://unused",
                    },
                    "summary": {
                        "rps": "fast",
                        "total_requests": 10,
                        "failed_requests": 0,
                    },
                    "latency_percentiles": {"p95_us": 1000.0},
                },
                id="string-rps",
            ),
        ],
    )
    def test_malformed_baseline_skips_comparison_but_writes_exports(
        self, local_server: str, tmp_path: Path, baseline_content: dict[str, object]
    ) -> None:
        baseline_file = tmp_path / "baseline.json"
        baseline_file.write_text(json.dumps(baseline_content))
        html_path = tmp_path / "report.html"
        md_path = tmp_path / "report.md"

        result = runner.invoke(
            app,
            [
                "load",
                local_server,
                "-c",
                "2",
                "-d",
                "1",
                "--no-progress",
                "--no-save",
                "--compare-to",
                str(baseline_file),
                "--html",
                str(html_path),
                "--markdown",
                str(md_path),
            ],
        )

        assert result.exit_code == 0, result.output
        assert "Skipping comparison" in result.output
        assert "Skipping comparison" in result.stderr
        assert "Traceback" not in result.output
        assert html_path.exists()
        assert md_path.exists()


class TestCLIJsonOutput:
    def test_json_load(self, local_server: str) -> None:
        result = runner.invoke(
            app,
            ["load", local_server, "-c", "2", "-d", "1", "--json", "--no-progress"],
        )
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert "total_requests" in data
        assert "average_latency_ms" in data
        assert data["url"] == local_server

    def test_json_stress(self, local_server: str) -> None:
        result = runner.invoke(
            app,
            [
                "stress",
                local_server,
                "--from",
                "2",
                "--to",
                "5",
                "--ramp",
                "1",
                "--hold",
                "1",
                "--json",
                "--no-progress",
            ],
        )
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert "total_requests" in data

    def test_json_spike(self, local_server: str) -> None:
        result = runner.invoke(
            app,
            [
                "spike",
                local_server,
                "--baseline",
                "1",
                "--peak",
                "3",
                "--spike-duration",
                "1",
                "--json",
                "--no-progress",
            ],
        )
        assert result.exit_code == 0
        data = json.loads(result.output)
        assert "total_requests" in data


class TestCLIBackwardCompat:
    def test_raw_url_defaults_to_load(self, local_server: str) -> None:
        with pytest.raises(SystemExit) as exc_info:
            main([local_server, "-c", "2", "-d", "1"])
        assert exc_info.value.code == 0

    def test_flags_before_url(self, local_server: str) -> None:
        with pytest.raises(SystemExit) as exc_info:
            main(["-c", "2", "-d", "1", local_server])
        assert exc_info.value.code == 0


class TestCLIVersion:
    def test_version_long_flag(self) -> None:
        result = runner.invoke(app, ["--version"])
        assert result.exit_code == 0
        assert "strobengine" in result.output

    def test_version_short_flag(self) -> None:
        result = runner.invoke(app, ["-V"])
        assert result.exit_code == 0
        assert "strobengine" in result.output


class TestCLIVerbosity:
    def test_verbose_flag(self, local_server: str) -> None:
        with mock.patch("strobengine.cli._configure_logging") as cfg:
            result = runner.invoke(
                app, ["load", "-v", local_server, "-c", "2", "-d", "1"]
            )
        assert result.exit_code == 0
        assert cfg.call_args.args[0] == "info"

    def test_double_verbose(self, local_server: str) -> None:
        with mock.patch("strobengine.cli._configure_logging") as cfg:
            result = runner.invoke(
                app, ["load", "-vv", local_server, "-c", "2", "-d", "1"]
            )
        assert result.exit_code == 0
        assert cfg.call_args.args[0] == "debug"

    def test_triple_verbose(self, local_server: str) -> None:
        with mock.patch("strobengine.cli._configure_logging") as cfg:
            result = runner.invoke(
                app, ["load", "-vvv", local_server, "-c", "2", "-d", "1"]
            )
        assert result.exit_code == 0
        assert cfg.call_args.args[0] == "trace"

    def test_quiet_flag(self, local_server: str) -> None:
        with mock.patch("strobengine.cli._configure_logging") as cfg:
            result = runner.invoke(
                app, ["load", "-q", local_server, "-c", "2", "-d", "1"]
            )
        assert result.exit_code == 0
        assert cfg.call_args.args[0] == "off"

    def test_no_progress_keeps_default_log_level(self, local_server: str) -> None:
        # --no-progress is a display flag, not a verbosity flag: it must not
        # raise the log level (True == 1 previously resolved to "info").
        with mock.patch("strobengine.cli._configure_logging") as cfg:
            result = runner.invoke(
                app, ["load", "--no-progress", local_server, "-c", "2", "-d", "1"]
            )
        assert result.exit_code == 0
        assert cfg.call_args.args[0] == "warn"

    def test_json_output_overrides_verbosity(self, local_server: str) -> None:
        with mock.patch("strobengine.cli._configure_logging") as cfg:
            result = runner.invoke(
                app, ["load", "--json", "-v", local_server, "-c", "2", "-d", "1"]
            )
        assert result.exit_code == 0
        assert cfg.call_args.args[0] == "off"

    def test_log_file_flag(self, local_server: str, tmp_path: Path) -> None:
        log_file = str(tmp_path / "test.log")
        result = runner.invoke(
            app,
            ["load", "-v", "--log-file", log_file, local_server, "-c", "2", "-d", "1"],
        )
        assert result.exit_code == 0
        assert Path(log_file).exists()


class TestCLIErrorHandling:
    def test_body_and_form_conflict_friendly_error(self) -> None:
        # --body and --form are mutually exclusive; the Rust build_engine
        # raises ValueError. The CLI must surface a friendly message and a
        # non-zero exit code instead of a raw Python traceback.
        result = runner.invoke(
            app,
            [
                "load",
                "http://unused",
                "-c",
                "1",
                "-d",
                "1",
                "--body",
                "x",
                "--form",
                "a=b",
            ],
        )
        assert result.exit_code == 1
        assert "Error:" in result.output
        assert "Traceback" not in result.output
