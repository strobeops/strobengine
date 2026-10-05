import json
import re
import subprocess
from pathlib import Path

import pytest


def _run_cli(
    cli_bin: str, args: list[str], timeout: int = 30
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [cli_bin, *args],
        capture_output=True,
        text=True,
        timeout=timeout,
    )


class TestSubcommands:
    @pytest.mark.parametrize(
        ("subcommand", "args"),
        [
            ("load", ["-c", "2", "-d", "2"]),
            ("stress", ["--from", "2", "--to", "4", "--ramp", "1", "--hold", "1"]),
            ("spike", ["--baseline", "2", "--peak", "4", "--spike-duration", "1"]),
        ],
    )
    def test_subcommands_run(
        self, cli_bin: str, mock_server: str, subcommand: str, args: list[str]
    ):
        full_args = [subcommand, f"{mock_server}/status/200", *args, "--no-progress"]
        result = _run_cli(cli_bin, full_args)
        assert result.returncode == 0, f"Process failed with stderr:\n{result.stderr}"


class TestExitCodes:
    @pytest.mark.parametrize(
        ("arg_name", "arg_val"),
        [
            ("-c", "0"),
            ("-d", "0"),
            ("--method", "INVALID"),
            ("--header", "NoColonHere"),
        ],
    )
    def test_invalid_arguments_fail(
        self, cli_bin: str, mock_server: str, arg_name: str, arg_val: str
    ):
        result = _run_cli(
            cli_bin,
            ["load", f"{mock_server}/status/200", arg_name, arg_val],
        )
        assert result.returncode != 0, f"Expected failure for {arg_name}={arg_val}"


class TestJsonOutput:
    def test_load_json_schema(self, cli_bin: str, mock_server: str):
        result = _run_cli(
            cli_bin,
            [
                "load",
                f"{mock_server}/status/200",
                "-c",
                "2",
                "-d",
                "2",
                "--json",
                "--no-progress",
            ],
        )
        assert result.returncode == 0

        data = json.loads(result.stdout)
        expected_keys = {
            "url",
            "total_requests",
            "total_errors",
            "average_latency_ms",
            "p95_latency_ms",
            "p99_latency_ms",
        }

        assert expected_keys.issubset(data.keys())
        assert data["url"] == f"{mock_server}/status/200"
        assert isinstance(data["total_requests"], int)
        assert isinstance(data["total_errors"], int)

    def test_stress_json_schema(self, cli_bin: str, mock_server: str):
        result = _run_cli(
            cli_bin,
            [
                "stress",
                f"{mock_server}/status/200",
                "--from",
                "2",
                "--to",
                "4",
                "--ramp",
                "1",
                "--hold",
                "1",
                "--json",
                "--no-progress",
            ],
        )
        assert result.returncode == 0

        data = json.loads(result.stdout)
        assert "total_requests" in data
        assert data["url"] == f"{mock_server}/status/200"

    def test_spike_json_schema(self, cli_bin: str, mock_server: str):
        result = _run_cli(
            cli_bin,
            [
                "spike",
                f"{mock_server}/status/200",
                "--baseline",
                "2",
                "--peak",
                "4",
                "--spike-duration",
                "1",
                "--json",
                "--no-progress",
            ],
        )
        assert result.returncode == 0

        data = json.loads(result.stdout)
        assert "total_requests" in data
        assert data["url"] == f"{mock_server}/status/200"

    def test_json_is_valid_no_extra_output(self, cli_bin: str, mock_server: str):
        result = _run_cli(
            cli_bin,
            [
                "load",
                f"{mock_server}/status/200",
                "-c",
                "2",
                "-d",
                "2",
                "--json",
                "--no-progress",
            ],
        )
        assert result.returncode == 0
        assert json.loads(result.stdout)
        assert not result.stderr.strip(), f"Unexpected stderr output:\n{result.stderr}"


class TestHtmlReport:
    def test_system_panel_present(self, cli_bin: str, mock_server: str, tmp_path: Path):
        report = tmp_path / "report.html"
        result = _run_cli(
            cli_bin,
            [
                "load",
                f"{mock_server}/status/200",
                "-c",
                "2",
                "-d",
                "3",
                "--no-progress",
                "--html",
                str(report),
            ],
        )
        assert result.returncode == 0, f"Process failed with stderr:\n{result.stderr}"
        html = report.read_text()
        # All three live inside the Jinja probe
        # `{% if system_metrics and system_metrics.samples %}`, so they prove the
        # Rust artifact path feeds the template the documented display shape.
        assert "Client Resource Footprint" in html
        assert "resourceChart" in html
        # At least one chart tick label from a non-empty samples[] array
        # (default --sys-sample-interval=1000 over -d 3 => samples at ~1.0s, ~2.0s).
        assert re.search(r"'\d+\.\d+s'", html)
