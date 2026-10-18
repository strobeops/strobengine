"""Relational database (SQL / JDBC-style) load testing examples.

Demonstrates connection-pool and query load benchmarking against PostgreSQL,
MySQL/MariaDB, and SQLite. JDBC is Java-only, so databases are reached through
native drivers; the backend is chosen from the URL scheme.

The SQLite examples run fully offline (no server) and create ``data.db`` in the
current directory on first use.

  PostgreSQL:  postgres://user:pass@localhost:5432/appdb
  MySQL:       mysql://user:pass@localhost:3306/appdb
  SQLite:      sqlite:///data.db?mode=rwc
"""

import sqlite3

from strobengine import RequestOptions, StrobEngine
from strobengine.reporter import print_summary

POSTGRES_URL = "postgres://user:pass@localhost:5432/appdb"
SQLITE_URL = "sqlite:///data.db?mode=rwc"


def _seed_sqlite() -> None:
    """Create ``data.db`` with a small table for the offline SQLite examples."""
    conn = sqlite3.connect("data.db")
    conn.execute("CREATE TABLE IF NOT EXISTS items (id INTEGER PRIMARY KEY, name TEXT)")
    if conn.execute("SELECT COUNT(*) FROM items").fetchone()[0] == 0:
        conn.executemany(
            "INSERT INTO items (name) VALUES (?)",
            [(f"item-{i}",) for i in range(10)],
        )
    conn.commit()
    conn.close()


def postgres_select_test():
    """Read benchmark with a bind parameter and a small connection pool."""
    engine = StrobEngine(
        url=POSTGRES_URL,
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
    print_summary(summary)


def sqlite_select_test():
    """Offline SQLite read benchmark (fetch mode counts returned rows)."""
    _seed_sqlite()
    engine = StrobEngine(
        url=SQLITE_URL,
        concurrency=5,
        duration=10,
        options=RequestOptions(
            db_query="SELECT id, name FROM items",
            db_fetch=True,
        ),
    )
    summary = engine.run()
    print_summary(summary)


def sqlite_insert_test():
    """Offline SQLite write benchmark (rows affected; pool size 1)."""
    _seed_sqlite()
    engine = StrobEngine(
        url=SQLITE_URL,
        concurrency=1,
        duration=10,
        options=RequestOptions(
            db_query="INSERT INTO items (name) VALUES (?)",
            db_params=["load-test"],
            db_pool_size=1,
        ),
    )
    summary = engine.run()
    print_summary(summary)


def chaos_test():
    """Read benchmark with chaos fault injection."""
    engine = StrobEngine(
        url=POSTGRES_URL,
        concurrency=10,
        duration=30,
        options=RequestOptions(
            db_query="SELECT id FROM items WHERE id = $1",
            db_params=["1"],
            db_fetch=True,
            chaos=True,
        ),
    )
    summary = engine.run()
    print_summary(summary)


def result_analysis():
    """Inspect pool-acquire vs query latency and row counts."""
    engine = StrobEngine(
        url=POSTGRES_URL,
        concurrency=10,
        duration=5,
        options=RequestOptions(
            db_query="SELECT id FROM items WHERE id = $1",
            db_params=["1"],
            db_fetch=True,
        ),
    )
    summary = engine.run()

    print(f"Total requests:      {summary.total_requests}")
    print(f"Total errors:        {summary.total_errors}")
    if summary.database is not None:
        print(f"Driver:              {summary.database.driver}")
        print(f"Queries:             {summary.database.queries_total}")
        print(f"Rows:                {summary.database.rows_total}")
        print(f"Pool acquire p99:    {summary.database.pool_acquire_p99_ms:.2f} ms")
        print(f"Query p99:           {summary.database.query_p99_ms:.2f} ms")


if __name__ == "__main__":
    postgres_select_test()
    sqlite_select_test()
    sqlite_insert_test()
    chaos_test()
    result_analysis()
