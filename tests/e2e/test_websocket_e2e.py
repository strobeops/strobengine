import asyncio

from strobengine.engine import RequestOptions, StrobEngine


class TestWebSocketLoadTest:
    async def test_successful_websocket_load(self, mock_server: str):
        ws_url = mock_server.replace("http://", "ws://") + "/ws"
        engine = StrobEngine(
            url=ws_url,
            concurrency=5,
            duration=2,
            options=RequestOptions(no_progress=True),
        )
        summary = await asyncio.wait_for(engine.run_async(), timeout=10.0)

        assert summary.total_requests > 0
        assert summary.total_errors == 0
        assert summary.average_latency_ms > 0

    async def test_websocket_unreachable_server(self):
        engine = StrobEngine(
            url="ws://127.0.0.1:59999",
            concurrency=2,
            duration=2,
            options=RequestOptions(no_progress=True),
        )
        summary = await asyncio.wait_for(engine.run_async(), timeout=10.0)

        assert summary.total_requests > 0
        assert summary.total_errors == summary.total_requests
        assert summary.status_codes.get(0, 0) == summary.total_requests

    async def test_websocket_ping_pong_mode(self, mock_server: str):
        ws_url = mock_server.replace("http://", "ws://") + "/ws"
        engine = StrobEngine(
            url=ws_url,
            concurrency=3,
            duration=2,
            options=RequestOptions(
                no_progress=True,
                ws_mode="ping_pong",
            ),
        )
        summary = await asyncio.wait_for(engine.run_async(), timeout=10.0)

        assert summary.total_requests > 0
        assert summary.total_errors == 0
        assert summary.average_latency_ms > 0

    async def test_websocket_custom_headers(self, mock_server: str):
        ws_url = mock_server.replace("http://", "ws://") + "/ws"
        engine = StrobEngine(
            url=ws_url,
            concurrency=2,
            duration=2,
            options=RequestOptions(
                no_progress=True,
                headers=[("X-Custom-Test", "e2e-value")],
            ),
        )
        summary = await asyncio.wait_for(engine.run_async(), timeout=10.0)

        assert summary.total_requests > 0
        assert summary.total_errors == 0
        assert summary.average_latency_ms > 0

    async def test_websocket_stream_mode_success(self, mock_server: str):
        ws_url = mock_server.replace("http://", "ws://") + "/ws"
        engine = StrobEngine(
            url=ws_url,
            concurrency=3,
            duration=2,
            options=RequestOptions(
                no_progress=True,
                ws_mode="stream",
                ws_payload='{"type": "echo", "data": "test"}',
            ),
        )
        summary = await asyncio.wait_for(engine.run_async(), timeout=10.0)

        assert summary.total_requests > 0
        assert summary.total_errors == 0
        assert summary.total_bytes_received > 0
        assert summary.duration_secs >= 1.5

    async def test_websocket_stream_default_payload(self, mock_server: str):
        ws_url = mock_server.replace("http://", "ws://") + "/ws"
        engine = StrobEngine(
            url=ws_url,
            concurrency=2,
            duration=2,
            options=RequestOptions(
                no_progress=True,
                ws_mode="stream",
            ),
        )
        summary = await asyncio.wait_for(engine.run_async(), timeout=10.0)

        assert summary.total_requests > 0
        assert summary.total_errors == 0
        assert summary.total_bytes_received > 0
        assert summary.duration_secs >= 1.5

    async def test_websocket_chaos_mode(self, mock_server: str):
        ws_url = mock_server.replace("http://", "ws://") + "/ws"
        engine = StrobEngine(
            url=ws_url,
            concurrency=3,
            duration=2,
            options=RequestOptions(
                no_progress=True,
                ws_mode="stream",
                ws_payload="hello",
                chaos=True,
            ),
        )
        summary = await asyncio.wait_for(engine.run_async(), timeout=10.0)

        # Chaos may cause some errors but engine should not crash
        assert summary.total_requests > 0
        assert summary.duration_secs >= 1.5
        # Assert chaos caused errors (ConnectionDrop or CorruptedPayload)
        assert summary.total_errors > 0
        # Assert status codes include chaos-related codes
        assert 0 in summary.status_codes or any(k >= 400 for k in summary.status_codes)

    async def test_websocket_continuous_streaming(self, mock_server: str):
        ws_url = mock_server.replace("http://", "ws://") + "/ws"
        engine = StrobEngine(
            url=ws_url,
            concurrency=2,
            duration=3,
            options=RequestOptions(
                no_progress=True,
                ws_mode="stream",
                ws_payload="hello",
            ),
        )
        summary = await asyncio.wait_for(engine.run_async(), timeout=10.0)

        # Multiple iterations should produce multiple requests
        assert summary.total_requests > 3
        assert summary.total_errors == 0
        assert summary.total_bytes_received > 0
        assert summary.average_latency_ms > 0

    async def test_websocket_pubsub_broadcasting(self, mock_server: str):
        ws_url = mock_server.replace("http://", "ws://") + "/ws/discard"
        # All workers are publishers; they send timestamped frames to the
        # broadcast server which fans them out to other connected clients.
        engine = StrobEngine(
            url=ws_url,
            concurrency=2,
            duration=3,
            options=RequestOptions(
                no_progress=True,
                ws_role="publisher",
                ws_publish_interval_ms=200,
                ws_subscribers=2,
            ),
        )
        summary = await asyncio.wait_for(engine.run_async(), timeout=15.0)

        assert summary.total_requests > 0
        assert summary.total_errors == 0

    async def test_websocket_pubsub_python_api(self, mock_server: str):
        ws_url = mock_server.replace("http://", "ws://") + "/ws/broadcast"
        engine = StrobEngine.load_test(
            url=ws_url,
            concurrency=2,
            duration=3,
            options=RequestOptions(
                no_progress=True,
                ws_role="subscriber",
                ws_subscribers=2,
            ),
        )
        summary = await asyncio.wait_for(engine.run_async(), timeout=15.0)

        assert summary.total_requests > 0
