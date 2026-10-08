import asyncio

from strobengine.engine import RequestOptions, StrobEngine


class TestGrpcE2E:
    async def test_grpc_unreachable_server(self):
        engine = StrobEngine(
            url="grpc://127.0.0.1:59999",
            concurrency=2,
            duration=2,
            options=RequestOptions(
                no_progress=True,
                timeout=1,
                grpc_service="test.Service",
                grpc_method="TestMethod",
            ),
        )
        summary = await asyncio.wait_for(engine.run_async(), timeout=5.0)

        assert summary.total_requests > 0
        assert summary.total_errors == summary.total_requests
        # Transport failures map to HTTP-equivalent error codes (503/500/...),
        # never a raw 0 that would masquerade as an unknown bucket.
        assert all(code >= 400 for code in summary.status_codes)

    async def test_grpc_chaos_mode(self):
        engine = StrobEngine(
            url="grpc://127.0.0.1:59999",
            concurrency=2,
            duration=2,
            options=RequestOptions(
                no_progress=True,
                timeout=1,
                grpc_service="test.Service",
                grpc_method="TestMethod",
                chaos=True,
                chaos_rate=1.0,
            ),
        )
        summary = await asyncio.wait_for(engine.run_async(), timeout=5.0)

        assert summary.total_requests > 0
        assert summary.total_errors > 0
        assert summary.duration_secs >= 1.5
        # Every iteration selects a fault at rate 1.0, and connect failures
        # after the selection must still be counted as injected faults.
        assert summary.chaos_injected_total > 0
        assert "ConnectionDrop" in summary.chaos_faults_by_type

    async def test_grpc_custom_headers(self):
        engine = StrobEngine(
            url="grpc://127.0.0.1:59999",
            concurrency=2,
            duration=2,
            options=RequestOptions(
                no_progress=True,
                timeout=1,
                grpc_service="test.Service",
                grpc_method="TestMethod",
                headers=[("Authorization", "Bearer token123")],
            ),
        )
        summary = await asyncio.wait_for(engine.run_async(), timeout=5.0)

        assert summary.total_requests > 0
        assert summary.total_errors == summary.total_requests

    async def test_grpc_deadline(self):
        engine = StrobEngine(
            url="grpc://127.0.0.1:59999",
            concurrency=2,
            duration=2,
            options=RequestOptions(
                no_progress=True,
                timeout=1,
                grpc_service="test.Service",
                grpc_method="TestMethod",
                grpc_deadline_ms=1000,
            ),
        )
        summary = await asyncio.wait_for(engine.run_async(), timeout=5.0)

        assert summary.total_requests > 0
        assert summary.total_errors == summary.total_requests
