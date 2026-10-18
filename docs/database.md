# Database (SQL / JDBC-style) Load Testing

strobengine benchmarks **relational databases** — the systems reached through
**JDBC** from the Java world — using native Rust drivers instead of JDBC itself
(JDBC is a Java API with no Rust equivalent). Connections are pooled with the
`sqlx` toolkit and each iteration runs one SQL statement, reporting query
throughput plus separate **connection-pool acquire latency** and **query
latency** so pool starvation is distinguishable from slow queries.

The backend is chosen from the connection URL scheme:

| Scheme | Backend |
|--------|---------|
| `postgres://`, `postgresql://` | PostgreSQL |
| `mysql://`, `mariadb://` | MySQL / MariaDB |
| `sqlite:`, `sqlite://` | SQLite (embedded, bundled) |

## Supported Features

| Feature | Description |
|---------|-------------|
| Connection pooling | `sqlx::AnyPool` with configurable `--db-pool-size` |
| Query load | One statement per iteration, execute or fetch mode |
| Separate latencies | Pool-acquire wait and query execution timed independently |
| Bind parameters | Positional text parameters via `--db-param` (repeatable) |
| Fetch mode | `--db-fetch` counts returned rows (streamed, capped by `--db-max-rows`) |
| TLS | rustls for PostgreSQL/MySQL (`tls-rustls-ring-webpki`) |
| Chaos Testing | Latency spikes, corrupted queries, metadata corruption, connection drops |

## Python API

Read benchmark (fetch mode):

```python
from strobengine import StrobEngine, RequestOptions

engine = StrobEngine(
    url="postgres://user:pass@localhost:5432/appdb",
    concurrency=20,
    duration=30,
    options=RequestOptions(
        db_query="SELECT id, name FROM items WHERE id = $1",
        db_params=["1"],
        db_pool_size=10,
        db_fetch=True,
    ),
)
summary = engine.run()

print(summary.database.queries_total)  # statements executed
print(summary.database.rows_total)  # rows returned
print(summary.database.pool_acquire_p99_ms)  # pool wait P99
print(summary.database.query_p99_ms)  # query execution P99
```

Write benchmark (execute mode):

```python
engine = StrobEngine(
    url="sqlite:///data.db?mode=rwc",
    concurrency=5,
    duration=30,
    options=RequestOptions(
        db_query="INSERT INTO events (kind, payload) VALUES (?, ?)",
        db_params=["click", "{}"],
        db_pool_size=1,
    ),
)
summary = engine.run()
print(summary.database.rows_total)  # rows affected
```

## CLI

```bash
# Read via PostgreSQL
strobengine load 'postgres://user:pass@localhost:5432/appdb' \
  --db-query 'SELECT id, name FROM items WHERE id = $1' \
  --db-param 1 \
  --db-fetch \
  --db-pool-size 10 \
  -c 20 -d 30

# Write via SQLite
strobengine load 'sqlite:///data.db?mode=rwc' \
  --db-query "INSERT INTO events (kind) VALUES ('click')" \
  --db-pool-size 1 \
  -c 5 -d 30
```

The database options also work with the `stress` and `spike` subcommands.

## Execute vs Fetch

| Mode | Flag | Executes | `rows_total` means |
|------|------|----------|--------------------|
| Execute (default) | — | `execute` | rows **affected** |
| Fetch | `--db-fetch` | `fetch` (streamed) | rows **returned** (capped by `--db-max-rows`) |

Fetch mode streams rows and stops counting at `--db-max-rows` (default 10,000;
`0` = unlimited), so a large result set cannot exhaust memory.

## Connection Pooling

`--db-pool-size` sets the maximum pooled connections (default: the concurrency
level). Each iteration acquires a connection from the pool, runs the query, and
releases it. Two independent latency distributions are reported:

- `pool_acquire_*_ms` — time spent waiting for a pooled connection. A rising
  tail here means the pool is the bottleneck (raise `--db-pool-size`).
- `query_*_ms` — time the database spent executing the statement.

The top-level `p95/p99_latency_ms` reflects the whole iteration.

## Parameters and Dialect

Parameters are bound as **text** and are always positional:

- Placeholder syntax is **backend-specific** and passed through verbatim:
  **PostgreSQL uses `$1`, `$2`, …**; **MySQL and SQLite use `?`**.
- Because values bind as text, the server applies its own implicit coercion to
  the target column type; coercion rules differ between backends (e.g. an
  integer-looking string into a numeric column).

## Metrics

The `database` report section exposes:

| Field | Meaning |
|-------|---------|
| `driver` | `postgres`, `mysql`, or `sqlite` |
| `queries_total` | Statements executed |
| `rows_total` | Rows affected or returned |
| `errors_total` | Connection or SQL errors |
| `pool_max` | Configured maximum pool size |
| `pool_size` | Pool size at the end of the run |
| `fetch_mode` | `execute` or `fetch` |
| `pool_acquire_*_ms` | Mean/p50/p90/p99 pool-acquire wait |
| `query_*_ms` | Mean/p50/p90/p99 query execution |

The query text is deliberately **never** written to reports (only the driver,
pool size, and fetch mode are recorded).

## Notes and Limitations

- **SQLite write contention:** a file database with `--db-pool-size > 1` can hit
  `SQLITE_BUSY` under concurrent writes. Use `--db-pool-size 1` for write
  benchmarks (reads are unaffected), or a WAL-enabled database file.
- **Create-if-missing:** append `?mode=rwc` to a `sqlite://` URL to create the
  database file on first connect.
- Dynamic SQL is operator-provided by design and executed as-is; use
  `--db-param` for data so values are bound rather than interpolated.
- Only single-statement prepared queries are used; multiple statements per
  query string are not supported.
