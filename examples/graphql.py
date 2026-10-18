"""GraphQL load testing examples.

Demonstrates queries and mutations over HTTP, plus subscriptions over
WebSockets (the ``graphql-transport-ws`` subprotocol). Requires a GraphQL
server that accepts ``POST application/json``; point the URLs below at your
own endpoint, e.g. a local ``graphql-http`` server:

  npx graphql-http-server --port 8080
"""

from strobengine import RequestOptions, StrobEngine
from strobengine.reporter import print_summary

GRAPHQL_HTTP_URL = "http://localhost:8080/graphql"
GRAPHQL_WS_URL = "ws://localhost:8080/graphql"

QUERY = "{ hero { name friends { name } } }"
MUTATION = "mutation AddHero($name: String!) { addHero(name: $name) { id } }"
SUBSCRIPTION = "subscription OnHero { heroAdded { id name } }"


def query_test():
    """Benchmark a GraphQL query (HTTP POST)."""
    engine = StrobEngine(
        url=GRAPHQL_HTTP_URL,
        concurrency=10,
        duration=30,
        options=RequestOptions(graphql_query=QUERY),
    )
    summary = engine.run()
    print_summary(summary)


def mutation_with_variables_test():
    """Mutation with variables and a named operation."""
    engine = StrobEngine(
        url=GRAPHQL_HTTP_URL,
        concurrency=10,
        duration=30,
        options=RequestOptions(
            graphql_query=MUTATION,
            graphql_variables='{"name": "R2-D2"}',
            graphql_operation_name="AddHero",
        ),
    )
    summary = engine.run()
    print_summary(summary)


def subscription_test():
    """GraphQL subscription over a WebSocket."""
    engine = StrobEngine(
        url=GRAPHQL_WS_URL,
        concurrency=5,
        duration=30,
        options=RequestOptions(graphql_query=SUBSCRIPTION),
    )
    summary = engine.run()
    print_summary(summary)
    gql = summary.graphql_ws
    if gql is not None:
        print(f"  messages: {gql.messages_total}  ttfm p50: {gql.ttfm_p50_ms:.2f} ms")


def chaos_test():
    """GraphQL query with chaos fault injection."""
    engine = StrobEngine(
        url=GRAPHQL_HTTP_URL,
        concurrency=10,
        duration=30,
        options=RequestOptions(graphql_query=QUERY, chaos=True),
    )
    summary = engine.run()
    print_summary(summary)


def result_analysis():
    """Inspect GraphQL operation and logical-error metrics."""
    engine = StrobEngine(
        url=GRAPHQL_HTTP_URL,
        concurrency=5,
        duration=5,
        options=RequestOptions(graphql_query=QUERY),
    )
    summary = engine.run()

    print(f"Total requests:      {summary.total_requests}")
    print(f"Total errors:        {summary.total_errors}")
    if summary.graphql is not None:
        print(f"Operations:          {summary.graphql.operations_total}")
        print(f"Error responses:     {summary.graphql.error_responses_total}")
        print(f"Operation type:      {summary.graphql.operation_type}")


if __name__ == "__main__":
    query_test()
    mutation_with_variables_test()
    subscription_test()
    chaos_test()
    result_analysis()
