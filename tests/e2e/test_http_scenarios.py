import aiohttp
import pytest

from strobengine.engine import RequestOptions, StrobEngine


class TestHighConcurrency:
    async def test_multiple_vus_hit_status_200(self, mock_server: str):
        engine = StrobEngine.load_test(
            url=f"{mock_server}/status/200",
            concurrency=4,
            duration=3,
            options=RequestOptions(no_progress=True),
        )
        summary = await engine.run_async()

        assert summary.total_requests > 0
        assert summary.total_errors == 0
        assert summary.average_latency_ms >= 0


class TestErrorCounting:
    async def test_500_status_counted_as_error(self, mock_server: str):
        engine = StrobEngine.load_test(
            url=f"{mock_server}/status/500",
            concurrency=2,
            duration=3,
            options=RequestOptions(no_progress=True),
        )
        summary = await engine.run_async()

        assert summary.total_requests > 0
        assert summary.total_errors == summary.total_requests


class TestTimeoutHandling:
    async def test_timeout_counts_errors_when_delay_exceeds_timeout(
        self, mock_server: str
    ):
        engine = StrobEngine.load_test(
            url=f"{mock_server}/delay/5",
            concurrency=2,
            duration=3,
            options=RequestOptions(timeout=1, no_progress=True),
        )
        summary = await engine.run_async()

        assert summary.total_requests > 0
        assert summary.total_errors == summary.total_requests


class TestPayloadAndMethodForwarding:
    @pytest.mark.parametrize("method", ["GET", "POST", "PUT", "PATCH", "DELETE"])
    async def test_http_methods_and_payload_forwarding(
        self, mock_server: str, method: str
    ):
        headers = [("X-Custom-Test", "e2e-value")]
        body = '{"key": "e2e-payload"}' if method in ("POST", "PUT", "PATCH") else None

        engine = StrobEngine.load_test(
            url=f"{mock_server}/echo",
            concurrency=1,
            duration=2,
            options=RequestOptions(
                method=method,
                body=body,
                headers=headers,
                no_progress=True,
            ),
        )
        summary = await engine.run_async()
        assert summary.total_errors == 0

        async with (
            aiohttp.ClientSession() as session,
            session.get(f"{mock_server}/last-echo") as resp,
        ):
            echo = await resp.json()

        assert echo["method"] == method
        assert echo["headers"].get("x-custom-test") == "e2e-value"

        if body:
            assert echo["body"] == {"key": "e2e-payload"}


class TestConnectionPoolMetrics:
    async def test_http_connection_pool_metrics(self, mock_server: str):
        """Verify connection pool metrics are populated after a load test run."""
        engine = StrobEngine.load_test(
            url=f"{mock_server}/status/200",
            concurrency=4,
            duration=2,
            options=RequestOptions(no_progress=True),
        )
        summary = await engine.run_async()

        assert summary.total_requests > 0
        # connection_reuse_ratio and avg_dns_resolution_ms should exist
        # and be non-negative floats (reqwest doesn't expose pool internals
        # so ratio may be 0, but it must not panic or be NaN)
        assert summary.connection_reuse_ratio >= 0.0
        assert summary.connection_reuse_ratio <= 1.0
        assert summary.avg_dns_resolution_ms >= 0.0
