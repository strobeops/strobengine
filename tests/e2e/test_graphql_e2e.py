import asyncio
import json

from strobengine.engine import RequestOptions, StrobEngine


class TestGraphqlE2E:
    async def test_graphql_query_metrics(self, mock_server: str):
        engine = StrobEngine(
            url=f"{mock_server}/graphql",
            concurrency=2,
            duration=2,
            options=RequestOptions(
                no_progress=True,
                graphql_query="{ hero { name } }",
                timeout=2,
            ),
        )
        summary = await asyncio.wait_for(engine.run_async(), timeout=15.0)

        assert summary.total_requests > 0
        assert summary.total_errors == 0
        assert summary.status_codes.get(200, 0) > 0
        assert summary.graphql is not None
        assert summary.graphql.operations_total > 0
        assert summary.graphql.error_responses_total == 0
        assert summary.graphql.errors_total == 0
        assert summary.graphql.operation_type == "query"

    async def test_graphql_mutation_operation_type(self, mock_server: str):
        engine = StrobEngine(
            url=f"{mock_server}/graphql",
            concurrency=2,
            duration=2,
            options=RequestOptions(
                no_progress=True,
                graphql_query="mutation { increment }",
                timeout=2,
            ),
        )
        summary = await asyncio.wait_for(engine.run_async(), timeout=15.0)

        assert summary.total_requests > 0
        assert summary.total_errors == 0
        assert summary.graphql is not None
        assert summary.graphql.operation_type == "mutation"

    async def test_graphql_errors_count_as_errors_on_http_200(self, mock_server: str):
        engine = StrobEngine(
            url=f"{mock_server}/graphql/errors",
            concurrency=2,
            duration=2,
            options=RequestOptions(
                no_progress=True,
                graphql_query="{ hero { name } }",
                timeout=2,
            ),
        )
        summary = await asyncio.wait_for(engine.run_async(), timeout=15.0)

        assert summary.total_requests > 0
        # GraphQL failures arrive as HTTP 200 + an "errors" array: the transport
        # succeeded, but the logical operation failed.
        assert summary.status_codes.get(200, 0) > 0
        assert summary.status_codes.get(0, 0) == 0
        assert summary.total_errors > 0
        assert summary.graphql is not None
        assert summary.graphql.error_responses_total > 0
        assert summary.graphql.errors_total >= summary.graphql.error_responses_total

    async def test_graphql_variables_and_operation_name_are_accepted(
        self, mock_server: str
    ):
        engine = StrobEngine(
            url=f"{mock_server}/graphql",
            concurrency=2,
            duration=2,
            options=RequestOptions(
                no_progress=True,
                graphql_query="query Hero($id: ID!) { hero(id: $id) { name } }",
                graphql_variables='{"id": "1"}',
                graphql_operation_name="Hero",
                timeout=2,
            ),
        )
        summary = await asyncio.wait_for(engine.run_async(), timeout=15.0)

        assert summary.total_requests > 0
        assert summary.total_errors == 0
        assert summary.graphql is not None
        assert summary.graphql.operations_total > 0

    async def test_graphql_metrics_json_export(self, mock_server: str):
        engine = StrobEngine(
            url=f"{mock_server}/graphql",
            concurrency=2,
            duration=2,
            options=RequestOptions(
                no_progress=True,
                graphql_query="{ hero { name } }",
                timeout=2,
            ),
        )
        summary = await asyncio.wait_for(engine.run_async(), timeout=15.0)

        data = json.loads(summary.to_json())
        assert "graphql" in data
        assert data["graphql"]["operations_total"] > 0
        assert data["graphql"]["operation_type"] == "query"
