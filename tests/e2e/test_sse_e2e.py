import asyncio
import json

from strobengine.engine import RequestOptions, StrobEngine


class TestSseE2E:
    async def test_sse_unreachable_server(self):
        engine = StrobEngine(
            url="http://127.0.0.1:59999/sse",
            concurrency=2,
            duration=2,
            options=RequestOptions(no_progress=True, sse_enabled=True, timeout=1),
        )
        summary = await asyncio.wait_for(engine.run_async(), timeout=10.0)

        assert summary.total_requests > 0
        assert summary.total_errors == summary.total_requests
        assert summary.status_codes.get(0, 0) == summary.total_requests

    async def test_sse_chaos_mode(self, mock_server: str):
        engine = StrobEngine(
            url=f"{mock_server}/sse",
            concurrency=3,
            duration=3,
            options=RequestOptions(
                no_progress=True,
                sse_enabled=True,
                chaos=True,
                timeout=1,
            ),
        )
        summary = await asyncio.wait_for(engine.run_async(), timeout=15.0)

        # Chaos may cause errors but engine should not crash
        assert summary.total_requests > 0
        assert summary.duration_secs >= 2.5

    async def test_sse_custom_headers(self, mock_server: str):
        engine = StrobEngine(
            url=f"{mock_server}/sse",
            concurrency=2,
            duration=2,
            options=RequestOptions(
                no_progress=True,
                sse_enabled=True,
                headers=[("X-Custom-Test", "e2e-value")],
            ),
        )
        summary = await asyncio.wait_for(engine.run_async(), timeout=10.0)

        assert summary.total_requests > 0
        assert summary.average_latency_ms >= 0

    async def test_sse_max_events_option(self, mock_server: str):
        engine = StrobEngine(
            url=f"{mock_server}/sse?count=5",
            concurrency=2,
            duration=3,
            options=RequestOptions(
                no_progress=True,
                sse_enabled=True,
                sse_max_events=3,
            ),
        )
        summary = await asyncio.wait_for(engine.run_async(), timeout=10.0)

        assert summary.total_requests > 0
        assert summary.duration_secs >= 1.0

    async def test_sse_scheme_resolution(self, mock_server: str):
        sse_url = mock_server.replace("http://", "sse://") + "/sse?count=3"
        engine = StrobEngine(
            url=sse_url,
            concurrency=2,
            duration=2,
            options=RequestOptions(no_progress=True),
        )
        summary = await asyncio.wait_for(engine.run_async(), timeout=10.0)

        assert summary.total_requests > 0
        assert summary.total_errors == 0

    async def test_sse_metrics_json_export(self, mock_server: str):
        engine = StrobEngine(
            url=f"{mock_server}/sse?count=5",
            concurrency=2,
            duration=3,
            options=RequestOptions(
                no_progress=True,
                sse_enabled=True,
                timeout=5,
            ),
        )
        summary = await asyncio.wait_for(engine.run_async(), timeout=15.0)

        # PyO3 getter attributes
        assert summary.sse is not None
        assert summary.sse.total_events_received > 0
        assert summary.sse.avg_ttfb_ms is not None
        assert summary.quic is None

        # JSON report export output
        json_data = json.loads(summary.to_json())
        assert "sse" in json_data
        assert json_data["sse"]["total_events_received"] > 0
        assert "avg_connection_latency_us" in json_data
        assert json_data["quic"] is None
