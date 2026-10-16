import asyncio
import json

from strobengine.engine import RequestOptions, SoapVersionEnum, StrobEngine

ENVELOPE = (
    '<?xml version="1.0"?>'
    '<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">'
    "<soap:Body><Add><a>1</a><b>2</b></Add></soap:Body></soap:Envelope>"
)


class TestSoapE2E:
    async def test_soap_success_metrics(self, mock_server: str):
        engine = StrobEngine(
            url=f"{mock_server}/soap",
            concurrency=2,
            duration=2,
            options=RequestOptions(
                no_progress=True,
                soap_enabled=True,
                soap_action="Add",
                body=ENVELOPE,
                timeout=2,
            ),
        )
        summary = await asyncio.wait_for(engine.run_async(), timeout=15.0)

        assert summary.total_requests > 0
        assert summary.total_errors == 0
        assert summary.status_codes.get(200, 0) > 0
        assert summary.soap is not None
        assert summary.soap.operations_total > 0
        assert summary.soap.fault_responses_total == 0
        assert summary.soap.soap_version == "1.1"

    async def test_soap11_fault_on_http_500(self, mock_server: str):
        engine = StrobEngine(
            url=f"{mock_server}/soap/fault500",
            concurrency=2,
            duration=2,
            options=RequestOptions(
                no_progress=True,
                soap_enabled=True,
                body=ENVELOPE,
                timeout=2,
            ),
        )
        summary = await asyncio.wait_for(engine.run_async(), timeout=15.0)

        # HTTP 500 is counted as an error; the fault is tracked in the soap section.
        assert summary.status_codes.get(500, 0) > 0
        assert summary.total_errors > 0
        assert summary.soap is not None
        assert summary.soap.fault_responses_total > 0

    async def test_soap12_fault_on_http_200_counts_as_error(self, mock_server: str):
        engine = StrobEngine(
            url=f"{mock_server}/soap/fault200",
            concurrency=2,
            duration=2,
            options=RequestOptions(
                no_progress=True,
                soap_enabled=True,
                soap_version=SoapVersionEnum.soap12,
                soap_action="Add",
                body=ENVELOPE,
                timeout=2,
            ),
        )
        summary = await asyncio.wait_for(engine.run_async(), timeout=15.0)

        # SOAP 1.2 faults arrive on HTTP 200: the transport is fine but the
        # operation failed, so total_errors must still rise.
        assert summary.status_codes.get(200, 0) > 0
        assert summary.total_errors > 0
        assert summary.soap is not None
        assert summary.soap.soap_version == "1.2"
        assert summary.soap.fault_responses_total > 0

    async def test_soap_metrics_json_export(self, mock_server: str):
        engine = StrobEngine(
            url=f"{mock_server}/soap",
            concurrency=2,
            duration=2,
            options=RequestOptions(
                no_progress=True,
                soap_enabled=True,
                soap_action="Add",
                body=ENVELOPE,
                timeout=2,
            ),
        )
        summary = await asyncio.wait_for(engine.run_async(), timeout=15.0)

        data = json.loads(summary.to_json())
        assert "soap" in data
        assert data["soap"]["operations_total"] > 0
        assert data["soap"]["soap_version"] == "1.1"

    async def test_soap_unreachable_server(self):
        engine = StrobEngine(
            url="http://127.0.0.1:59998/soap",
            concurrency=1,
            duration=1,
            options=RequestOptions(
                no_progress=True,
                soap_enabled=True,
                body=ENVELOPE,
                timeout=1,
            ),
        )
        summary = await asyncio.wait_for(engine.run_async(), timeout=15.0)

        assert summary.total_requests > 0
        assert summary.total_errors == summary.total_requests
        assert summary.status_codes.get(0, 0) == summary.total_requests
