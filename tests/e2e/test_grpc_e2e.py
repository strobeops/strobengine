from strobengine.engine import RequestOptions, StrobEngine


class TestGrpcE2E:
    async def test_grpc_unreachable_server(self):
        engine = StrobEngine(
            url="grpc://127.0.0.1:59999",
            concurrency=2,
            duration=2,
            options=RequestOptions(
                no_progress=True,
                grpc_service="test.Service",
                grpc_method="TestMethod",
            ),
        )
        summary = await engine.run_async()

        assert summary.total_requests > 0
        assert summary.total_errors == summary.total_requests
        assert summary.status_codes.get(0, 0) == summary.total_requests
