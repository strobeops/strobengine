import asyncio
import json

import pytest

from strobengine.engine import RequestOptions, StrobEngine


@pytest.mark.asyncio
async def test_latency_stats_json_output(mock_server: str):
    engine = StrobEngine(
        url=f"{mock_server}/status/200",
        concurrency=5,
        duration=3,
        options=RequestOptions(no_progress=True, timeout=5),
    )
    summary = await asyncio.wait_for(engine.run_async(), timeout=15.0)

    # Parse JSON output
    json_data = json.loads(summary.to_json())

    # Validate extended latency fields
    assert "std_dev_latency_ms" in json_data
    assert json_data["std_dev_latency_ms"] >= 0
    assert "p99_99_latency_ms" in json_data
    assert json_data["p99_99_latency_ms"] >= json_data.get("p99_latency_ms", 0.0)
    assert "latency_histogram" in json_data
    assert isinstance(json_data["latency_histogram"], dict)

    # Validate histogram bucket counts sum to total requests recorded
    total_in_histogram = sum(json_data["latency_histogram"].values())
    assert total_in_histogram == summary.total_requests


@pytest.mark.asyncio
async def test_latency_stats_terminal_output(mock_server: str, capsys):
    engine = StrobEngine(
        url=f"{mock_server}/status/200",
        concurrency=5,
        duration=3,
        options=RequestOptions(no_progress=True, timeout=5),
    )
    summary = await asyncio.wait_for(engine.run_async(), timeout=15.0)

    from strobengine.reporter import print_summary

    print_summary(summary)

    captured = capsys.readouterr()
    # Histogram should appear in output when there are requests
    if summary.total_requests > 0:
        assert "Latency Distribution" in captured.out
