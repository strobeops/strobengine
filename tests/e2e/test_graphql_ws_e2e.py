import asyncio
import json

from strobengine.engine import RequestOptions, StrobEngine


def _ws_url(mock_server: str, path: str) -> str:
    return mock_server.replace("http://", "ws://") + path


class TestGraphqlWsE2E:
    async def test_subscription_records_metrics(self, mock_server: str):
        engine = StrobEngine(
            url=_ws_url(mock_server, "/graphql/ws"),
            concurrency=2,
            duration=2,
            options=RequestOptions(
                no_progress=True,
                graphql_query="subscription { ticks }",
                timeout=2,
            ),
        )
        summary = await asyncio.wait_for(engine.run_async(), timeout=15.0)

        assert summary.total_requests > 0
        assert summary.total_errors == 0
        assert summary.graphql_ws is not None
        assert summary.graphql_ws.operations_total > 0
        assert summary.graphql_ws.messages_total > 0
        assert summary.graphql_ws.errors_total == 0
        # TTFM / interval are measured in milliseconds from HDR histograms.
        assert summary.graphql_ws.ttfm_mean_ms >= 0
        assert summary.graphql_ws.ttfm_p99_ms >= summary.graphql_ws.ttfm_p50_ms
        assert summary.graphql_ws.interval_p90_ms >= 0
        # WebSocket runs must not populate the HTTP GraphQL section.
        assert summary.graphql is None

    async def test_error_frames_count_as_errors_on_http_200(self, mock_server: str):
        engine = StrobEngine(
            url=_ws_url(mock_server, "/graphql/ws/errors"),
            concurrency=2,
            duration=2,
            options=RequestOptions(
                no_progress=True,
                graphql_query="subscription { ticks }",
                timeout=2,
            ),
        )
        summary = await asyncio.wait_for(engine.run_async(), timeout=15.0)

        assert summary.total_requests > 0
        assert summary.total_errors > 0
        assert summary.status_codes.get(0, 0) == 0
        assert summary.graphql_ws is not None
        assert summary.graphql_ws.errors_total > 0

    async def test_json_export_contains_graphql_ws(self, mock_server: str):
        engine = StrobEngine(
            url=_ws_url(mock_server, "/graphql/ws"),
            concurrency=2,
            duration=2,
            options=RequestOptions(
                no_progress=True,
                graphql_query="subscription { ticks }",
                timeout=2,
            ),
        )
        summary = await asyncio.wait_for(engine.run_async(), timeout=15.0)

        data = json.loads(summary.to_json())
        assert "graphql_ws" in data
        assert data["graphql_ws"]["messages_total"] > 0
        assert "ttfm_mean_ms" in data["graphql_ws"]
        assert "interval_p99_ms" in data["graphql_ws"]

    async def test_unreachable_server_is_error(self):
        engine = StrobEngine(
            url="ws://127.0.0.1:59998/graphql/ws",
            concurrency=2,
            duration=2,
            options=RequestOptions(
                no_progress=True,
                graphql_query="subscription { ticks }",
                timeout=1,
            ),
        )
        summary = await asyncio.wait_for(engine.run_async(), timeout=15.0)

        assert summary.total_requests > 0
        assert summary.total_errors == summary.total_requests
        assert summary.status_codes.get(0, 0) == summary.total_requests
