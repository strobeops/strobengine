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
        summary = await engine.run_async()

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
        summary = await engine.run_async()

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
        summary = await engine.run_async()

        assert summary.total_requests > 0
        assert summary.total_errors == 0
        assert summary.average_latency_ms > 0
        # PingPong mode should receive pong responses
        assert summary.total_bytes_received > 0

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
        summary = await engine.run_async()

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
        summary = await engine.run_async()

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
        summary = await engine.run_async()

        assert summary.total_requests > 0
        assert summary.total_errors == 0
        assert summary.total_bytes_received > 0
        assert summary.duration_secs >= 1.5
