import json
from unittest.mock import Mock

from strobengine.artifact import build_artifact_dict
from strobengine.engine import RequestOptions, StrobEngine


def _key_shape(value):
    """Recursively reduce a JSON-ish value to its key structure (leaves dropped)."""
    if isinstance(value, dict):
        return {k: _key_shape(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_key_shape(v) for v in value]
    return None


def _strip_system_info(artifact):
    """Copy an artifact with metadata.system_info removed (sources differ by design)."""
    metadata = {k: v for k, v in artifact["metadata"].items() if k != "system_info"}
    return {**artifact, "metadata": metadata}


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

        report_files = [
            f for f in reports_dir.glob("*.json") if f.name != "latest.json"
        ]
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
        report_files = [f for f in custom_dir.glob("*.json") if f.name != "latest.json"]
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

    async def test_saved_report_path_property(
        self, mock_server: str, tmp_path, monkeypatch
    ):
        """engine.saved_report_path returns the correct path after run."""
        monkeypatch.chdir(tmp_path)

        engine = StrobEngine(
            url=mock_server,
            concurrency=2,
            duration=1,
            options=RequestOptions(no_progress=True),
        )
        await engine.run_async()

        assert engine.saved_report_path is not None
        assert engine.saved_report_path.endswith(".json")
        assert "latest.json" not in engine.saved_report_path

    async def test_saved_report_path_none_when_no_save(
        self, mock_server: str, tmp_path, monkeypatch
    ):
        """engine.saved_report_path is None when no_save=True."""
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

        assert engine.saved_report_path is None


class TestArtifactParityE2E:
    async def test_rust_fallback_artifact_parity(
        self, mock_server: str, tmp_path, monkeypatch
    ):
        """Rust-path and fallback-path artifacts mirror each other for one real TestSummary.

        The Rust path needs a real TestSummary (not constructible from Python),
        so this is the cross-path parity check that the unit suite cannot do.
        """
        monkeypatch.chdir(tmp_path)

        engine = StrobEngine(
            url=mock_server,
            concurrency=2,
            duration=1,
            options=RequestOptions(no_progress=True),
        )
        summary = await engine.run_async()

        rust = build_artifact_dict(summary, engine.config)

        # Same summary through the Python fallback (config not a Rust TestConfig).
        # chaos_rate must be the 0.1 literal: Rust serde emits the f32 shortest
        # repr ("0.1"), while the widened f32 getter would give 0.10000000149011612.
        cfg = engine.config
        fallback = build_artifact_dict(
            summary,
            Mock(
                url=cfg.url,
                method=cfg.method,
                concurrency=cfg.concurrency,
                timeout_secs=cfg.timeout_secs,
                chaos=cfg.chaos,
                chaos_rate=0.1,
                body=cfg.body,
                headers=cfg.headers,
            ),
        )

        # Identical key structure everywhere (both paths apply the omit contract).
        assert _key_shape(rust) == _key_shape(fallback)

        # Identical values everywhere except system_info, whose sources differ by
        # design (Rust: HOSTNAME/consts::OS/CARGO_PKG_VERSION vs Python:
        # gethostname()/sys.platform/importlib.metadata).
        assert _strip_system_info(rust) == _strip_system_info(fallback)

        # Absent optional blocks are omitted, never emitted as null.
        for key in ("quic", "sse", "websocket", "grpc", "http3", "chaos_faults"):
            assert key not in rust
            assert key not in fallback
