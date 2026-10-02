"""Per-iteration deadline: a stalled target must not hang the run.

Regression test for the missing per-iteration timeout in ``spawn_worker``:
a blackholed TCP target parked workers forever, so the CLI only ever
exited via Ctrl+C. Every iteration is now bounded by ``--timeout``.
"""

import json
import subprocess


class TestIterationDeadline:
    def test_blackhole_target_completes_under_timeout(
        self, cli_bin: str, blackhole_server: str
    ):
        # subprocess timeout is the regression guard: without the fix this
        # run never returns and TimeoutExpired fails the test cleanly.
        result = subprocess.run(
            [
                cli_bin,
                "load",
                f"{blackhole_server}/",
                "-c",
                "2",
                "-d",
                "2",
                "-t",
                "1",
                "--json",
                "--no-progress",
            ],
            capture_output=True,
            text=True,
            timeout=20,
        )
        assert result.returncode == 0, f"Process failed with stderr:\n{result.stderr}"

        data = json.loads(result.stdout)
        assert data["total_requests"] > 0
        # Every iteration hits the 1s deadline against the silent server.
        assert data["total_errors"] == data["total_requests"]
