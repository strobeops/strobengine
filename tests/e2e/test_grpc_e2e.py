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
        summary = await engine.run_async()

        assert summary.total_requests > 0
        assert summary.total_errors == summary.total_requests
        assert summary.status_codes.get(0, 0) == summary.total_requests

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
            ),
        )
        summary = await engine.run_async()

        assert summary.total_requests > 0
        assert summary.total_errors > 0
        assert summary.duration_secs >= 1.5

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
        summary = await engine.run_async()

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
        summary = await engine.run_async()

        assert summary.total_requests > 0
        assert summary.total_errors == summary.total_requests
