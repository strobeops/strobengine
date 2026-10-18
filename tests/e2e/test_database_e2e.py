import asyncio
import json
import sqlite3
import subprocess

import pytest

from strobengine.engine import RequestOptions, StrobEngine


def _seed_db(path) -> str:
    """Create a SQLite file with a small table and return its URL."""
    conn = sqlite3.connect(path)
    conn.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)")
    conn.executemany(
        "INSERT INTO items (name) VALUES (?)",
        [(f"item-{i}",) for i in range(5)],
    )
    conn.commit()
    conn.close()
    return f"sqlite://{path}?mode=rwc"


def _options(**overrides) -> RequestOptions:
    defaults = {"no_progress": True, "timeout": 5}
    defaults.update(overrides)
    return RequestOptions(**defaults)


class TestDatabaseE2E:
    async def test_select_counts_rows(self, tmp_path):
        url = _seed_db(tmp_path / "select.db")
        engine = StrobEngine(
            url=url,
            concurrency=2,
            duration=1,
            options=_options(db_query="SELECT id FROM items", db_fetch=True),
        )
        summary = await asyncio.wait_for(engine.run_async(), timeout=30.0)

        assert summary.total_requests > 0
        assert summary.total_errors == 0
        assert summary.database is not None
        assert summary.database.driver == "sqlite"
        assert summary.database.fetch_mode == "fetch"
        assert summary.database.queries_total > 0
        assert summary.database.rows_total > 0
        assert summary.database.pool_size >= 1

    async def test_bind_parameters(self, tmp_path):
        url = _seed_db(tmp_path / "bind.db")
        engine = StrobEngine(
            url=url,
            concurrency=1,
            duration=1,
            options=_options(
                db_query="SELECT id FROM items WHERE id = ?",
                db_fetch=True,
                db_params=["1"],
            ),
        )
        summary = await asyncio.wait_for(engine.run_async(), timeout=30.0)
        assert summary.total_errors == 0
        assert summary.database is not None
        assert summary.database.rows_total > 0

    async def test_execute_reports_rows_affected(self, tmp_path):
        url = _seed_db(tmp_path / "execute.db")
        engine = StrobEngine(
            url=url,
            concurrency=1,
            duration=1,
            options=_options(
                db_query="UPDATE items SET name = 'x'",
                db_fetch=False,
            ),
        )
        summary = await asyncio.wait_for(engine.run_async(), timeout=30.0)
        assert summary.total_errors == 0
        assert summary.database is not None
        assert summary.database.fetch_mode == "execute"
        assert summary.database.rows_total > 0

    async def test_bad_query_is_error(self, tmp_path):
        url = _seed_db(tmp_path / "bad.db")
        engine = StrobEngine(
            url=url,
            concurrency=1,
            duration=1,
            options=_options(db_query="SELECT * FROM does_not_exist", db_fetch=True),
        )
        summary = await asyncio.wait_for(engine.run_async(), timeout=30.0)
        assert summary.total_errors == summary.total_requests
        assert summary.database is not None
        assert summary.database.errors_total > 0

    async def test_database_metrics_json_export(self, tmp_path):
        url = _seed_db(tmp_path / "json.db")
        engine = StrobEngine(
            url=url,
            concurrency=1,
            duration=1,
            options=_options(db_query="SELECT id FROM items", db_fetch=True),
        )
        summary = await asyncio.wait_for(engine.run_async(), timeout=30.0)
        data = json.loads(summary.to_json())
        assert "database" in data
        assert data["database"]["driver"] == "sqlite"
        assert data["database"]["queries_total"] > 0


class TestDatabaseCli:
    def test_cli_select(self, cli_bin: str, tmp_path):
        url = _seed_db(tmp_path / "cli.db")
        result = subprocess.run(
            [
                cli_bin,
                "load",
                url,
                "--db-query",
                "SELECT id FROM items",
                "--db-fetch",
                "-c",
                "1",
                "-d",
                "1",
                "--no-progress",
                "--no-save",
            ],
            capture_output=True,
            text=True,
            timeout=60,
        )
        assert result.returncode == 0, result.stderr


class TestDatabaseValidation:
    def test_db_url_requires_query(self, tmp_path):
        with pytest.raises(ValueError):
            StrobEngine(
                url="sqlite:///:memory:",
                concurrency=1,
                duration=1,
                options=_options(),
            )
