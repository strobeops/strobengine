import asyncio
import json
import subprocess

import pytest

from strobengine.engine import (
    RequestOptions,
    StompAckEnum,
    StompModeEnum,
    StrobEngine,
)


def _produce_options(**overrides) -> RequestOptions:
    defaults = {
        "no_progress": True,
        "stomp_destination": "/queue/orders",
        "stomp_message": "hello",
        "timeout": 3,
    }
    defaults.update(overrides)
    return RequestOptions(**defaults)


class TestStompE2E:
    async def test_produce_fire_and_forget(self, stomp_server):
        engine = StrobEngine(
            url=stomp_server.url,
            concurrency=2,
            duration=1,
            options=_produce_options(),
        )
        summary = await asyncio.wait_for(engine.run_async(), timeout=20.0)

        assert summary.total_requests > 0
        assert summary.total_errors == 0
        assert summary.status_codes.get(200, 0) == summary.total_requests
        assert summary.stomp is not None
        assert summary.stomp.messages_sent_total > 0
        assert summary.stomp.destination == "/queue/orders"
        assert summary.stomp.mode == "produce"
        assert summary.stomp.receipts_enabled is False

    async def test_produce_with_receipt_reports_ack_latency(self, stomp_server):
        engine = StrobEngine(
            url=stomp_server.url,
            concurrency=1,
            duration=1,
            options=_produce_options(stomp_receipt=True),
        )
        summary = await asyncio.wait_for(engine.run_async(), timeout=20.0)

        assert summary.total_errors == 0
        assert summary.stomp is not None
        assert summary.stomp.receipts_enabled is True
        assert summary.stomp.messages_sent_total > 0
        assert summary.stomp.send_latency_mean_ms >= 0.0

    async def test_consume_receives_messages(self, stomp_server):
        engine = StrobEngine(
            url=stomp_server.url,
            concurrency=2,
            duration=1,
            options=RequestOptions(
                no_progress=True,
                stomp_destination="/queue/orders",
                stomp_mode=StompModeEnum.consume,
                stomp_ack=StompAckEnum.auto,
                timeout=3,
            ),
        )
        summary = await asyncio.wait_for(engine.run_async(), timeout=20.0)

        assert summary.stomp is not None
        assert summary.stomp.mode == "consume"
        assert summary.stomp.ack_mode == "auto"
        assert summary.stomp.messages_received_total > 0

    async def test_stomp_metrics_json_export(self, stomp_server):
        engine = StrobEngine(
            url=stomp_server.url,
            concurrency=1,
            duration=1,
            options=_produce_options(),
        )
        summary = await asyncio.wait_for(engine.run_async(), timeout=20.0)

        data = json.loads(summary.to_json())
        assert "stomp" in data
        assert data["stomp"]["messages_sent_total"] > 0
        assert data["stomp"]["destination"] == "/queue/orders"

    async def test_unreachable_broker_is_error(self):
        engine = StrobEngine(
            url="stomp://127.0.0.1:59997",
            concurrency=1,
            duration=1,
            options=_produce_options(timeout=1),
        )
        summary = await asyncio.wait_for(engine.run_async(), timeout=20.0)

        assert summary.total_requests > 0
        assert summary.total_errors == summary.total_requests
        assert summary.status_codes.get(0, 0) == summary.total_requests
        assert summary.stomp is not None
        assert summary.stomp.errors_total > 0


class TestStompCli:
    def test_cli_produce(self, cli_bin: str, stomp_server):
        result = subprocess.run(
            [
                cli_bin,
                "load",
                stomp_server.url,
                "--stomp-destination",
                "/queue/orders",
                "--stomp-message",
                "hello",
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


class TestStompValidation:
    def test_stomp_url_requires_destination(self, stomp_server):
        with pytest.raises(ValueError):
            StrobEngine(
                url=stomp_server.url,
                concurrency=1,
                duration=1,
                options=RequestOptions(no_progress=True),
            )

    def test_stomps_tls_is_rejected(self):
        engine = StrobEngine(
            url="stomps://127.0.0.1:61613",
            concurrency=1,
            duration=1,
            options=_produce_options(),
        )
        with pytest.raises(ValueError, match="TLS"):
            engine.run()
