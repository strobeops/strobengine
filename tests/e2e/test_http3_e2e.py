import asyncio

from strobengine.engine import RequestOptions, StrobEngine


class TestHttp3E2E:
    async def test_http3_unreachable_server(self):
        engine = StrobEngine(
            url="h3://127.0.0.1:59999",
            concurrency=2,
            duration=2,
            options=RequestOptions(
                no_progress=True,
                timeout=1,
                http3_enabled=True,
            ),
        )
        summary = await asyncio.wait_for(engine.run_async(), timeout=15.0)

        assert summary.total_requests > 0
        assert summary.total_errors == summary.total_requests
        assert summary.status_codes.get(0, 0) == summary.total_requests

    async def test_http3_chaos_mode(self):
        engine = StrobEngine(
            url="h3://127.0.0.1:59999",
            concurrency=2,
            duration=2,
            options=RequestOptions(
                no_progress=True,
                timeout=1,
                http3_enabled=True,
                chaos=True,
            ),
        )
        summary = await asyncio.wait_for(engine.run_async(), timeout=15.0)

        assert summary.total_requests > 0
        assert summary.total_errors > 0
        assert summary.duration_secs >= 1.5

    async def test_http3_custom_headers(self):
        engine = StrobEngine(
            url="h3://127.0.0.1:59999",
            concurrency=2,
            duration=2,
            options=RequestOptions(
                no_progress=True,
                timeout=1,
                http3_enabled=True,
                headers=[("Authorization", "Bearer token123")],
            ),
        )
        summary = await asyncio.wait_for(engine.run_async(), timeout=15.0)

        assert summary.total_requests > 0
        assert summary.total_errors == summary.total_requests

    async def test_http3_zero_rtt_option(self):
        engine = StrobEngine(
            url="h3://127.0.0.1:59999",
            concurrency=2,
            duration=2,
            options=RequestOptions(
                no_progress=True,
                timeout=1,
                http3_enabled=True,
                quic_zero_rtt=True,
            ),
        )
        summary = await asyncio.wait_for(engine.run_async(), timeout=15.0)

        assert summary.total_requests > 0
        assert summary.total_errors == summary.total_requests
