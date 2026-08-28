import json

from strobengine.engine import RequestOptions, StrobEngine


class TestPersistenceE2E:
    async def test_default_creates_report_dir(
        self, mock_server: str, tmp_path, monkeypatch
    ):
        """Default execution implicitly creates .strobengine/reports/ with valid JSON."""
        monkeypatch.chdir(tmp_path)

        engine = StrobEngine(
            url=mock_server,
            concurrency=2,
            duration=1,
            options=RequestOptions(no_progress=True),
        )
        await engine.run_async()

        reports_dir = tmp_path / ".strobengine" / "reports"
        assert reports_dir.exists() and reports_dir.is_dir()

        report_files = list(reports_dir.glob("report_*.json"))
        assert len(report_files) >= 1

        # Validate JSON schema & contents
        payload = json.loads(report_files[0].read_text())
        assert "metadata" in payload
        assert "summary" in payload
        assert "latency_percentiles" in payload
        assert "error_breakdown" in payload
        assert payload["metadata"]["target_url"] == mock_server

        # Check latest.json pointer
        latest_file = reports_dir / "latest.json"
        assert latest_file.exists()
        latest_payload = json.loads(latest_file.read_text())
        assert latest_payload["latest_report"] == report_files[0].name

    async def test_custom_output_dir(self, mock_server: str, tmp_path, monkeypatch):
        """Custom output_dir writes report into specified directory."""
        monkeypatch.chdir(tmp_path)
        custom_dir = tmp_path / "custom_artifacts"

        engine = StrobEngine(
            url=mock_server,
            concurrency=2,
            duration=1,
            options=RequestOptions(
                no_progress=True,
                output_dir=str(custom_dir),
            ),
        )
        await engine.run_async()

        assert custom_dir.exists()
        report_files = list(custom_dir.glob("report_*.json"))
        assert len(report_files) >= 1

        # Confirm default directory was NOT created
        default_dir = tmp_path / ".strobengine" / "reports"
        assert not default_dir.exists()

    async def test_no_save_bypasses_write(
        self, mock_server: str, tmp_path, monkeypatch
    ):
        """no_save=True bypasses disk persistence entirely."""
        monkeypatch.chdir(tmp_path)

        engine = StrobEngine(
            url=mock_server,
            concurrency=2,
            duration=1,
            options=RequestOptions(
                no_progress=True,
                no_save=True,
            ),
        )
        await engine.run_async()

        default_dir = tmp_path / ".strobengine"
        assert not default_dir.exists()
