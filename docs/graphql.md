# GraphQL Load Testing

strobengine benchmarks GraphQL operations over standard HTTP. A GraphQL query
or mutation document is POSTed as `application/json` and GraphQL-level
`errors` are tracked separately from HTTP transport failures.

## Supported Features

| Feature | Description |
|---------|-------------|
| Queries & Mutations | Benchmark any query/mutation document over HTTP POST |
| Variables | Pass a JSON object of variables via `graphql_variables` |
| Named Operations | Select an operation with `graphql_operation_name` |
| Logical Error Tracking | Count responses whose `errors` array is non-empty |
| Operation Type | Reports `query`, `mutation`, or `subscription` |
| Custom Headers | Metadata / auth injection via request headers |
| Chaos Testing | Latency spikes, corrupted payloads, connection drops |

GraphQL requests always use `POST` with `Content-Type: application/json`; the
`method`, `body`, and `form` options are ignored (and combining `body`/`form`
with `graphql_query` is rejected).

## Python API

### Benchmark a Query

```python
from strobengine import StrobEngine, RequestOptions

engine = StrobEngine(
    url="http://localhost:8080/graphql",
    concurrency=10,
    duration=30,
    options=RequestOptions(
        graphql_query="{ hero { name friends { name } } }",
    ),
)
summary = engine.run()

print(summary.graphql.operations_total)       # GraphQL operations observed
print(summary.graphql.error_responses_total)  # responses with a non-empty errors[]
print(summary.graphql.operation_type)         # "query"
```

### Benchmark a Mutation with Variables

```python
options = RequestOptions(
    graphql_query="mutation AddHero($name: String!) { addHero(name: $name) { id } }",
    graphql_variables='{"name": "R2-D2"}',
    graphql_operation_name="AddHero",
)
```

## CLI

```bash
strobengine load http://localhost:8080/graphql \
  --graphql-query '{ hero { name } }' \
  -c 10 -d 30

strobengine load http://localhost:8080/graphql \
  --graphql-query 'mutation AddHero($name: String!) { addHero(name: $name) { id } }' \
  --graphql-variables '{"name": "R2-D2"}' \
  --graphql-operation-name AddHero \
  -c 10 -d 30
```

`--graphql-query` also works with the `stress` and `spike` subcommands.

## Error Semantics

GraphQL reports logical failures as HTTP 200 with a non-empty `errors` array.
strobengine counts such a response toward `total_errors` (so the error rate
reflects failed operations, not just failed transports) while keeping the
HTTP `status_codes` breakdown truthful. The `graphql` report section exposes:

| Field | Meaning |
|-------|---------|
| `operations_total` | Responses parsed as GraphQL operations |
| `error_responses_total` | Responses with a non-empty `errors` array |
| `errors_total` | Total entries across all `errors` arrays |
| `operation_type` | `query`, `mutation`, or `subscription` (from the document) |

## Notes

- Responses that are not GraphQL (non-JSON, or JSON without `data`/`errors`)
  are treated as ordinary HTTP responses and do not add to `operations_total`.
- `operation_type` is derived from the configured document's first operation
  keyword; the anonymous `{ ... }` shorthand is treated as a `query`.
