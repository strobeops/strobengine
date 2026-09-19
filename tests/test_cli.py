import json
from pathlib import Path

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
        result = runner.invoke(app, ["load", "-v", local_server, "-c", "2", "-d", "1"])
        assert result.exit_code == 0

    def test_double_verbose(self, local_server: str) -> None:
        result = runner.invoke(app, ["load", "-vv", local_server, "-c", "2", "-d", "1"])
        assert result.exit_code == 0

    def test_quiet_flag(self, local_server: str) -> None:
        result = runner.invoke(app, ["load", "-q", local_server, "-c", "2", "-d", "1"])
        assert result.exit_code == 0

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
