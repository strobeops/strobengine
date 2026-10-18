"""SOAP web service load testing examples.

Demonstrates SOAP 1.1 and 1.2 envelopes with operation addressing, fault
handling, and chaos injection. Requires a SOAP endpoint; for a local demo any
server that echoes the posted XML works, e.g. go-httpbin:

  podman run -d --rm -p 8080:8080 docker.io/mccutchen/go-httpbin
"""

from strobengine import RequestOptions, StrobEngine
from strobengine.engine import SoapVersionEnum
from strobengine.reporter import print_summary

SOAP_URL = "http://localhost:8080/soap"

ENVELOPE = (
    '<?xml version="1.0"?>'
    '<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">'
    "<soap:Body><Add><a>1</a><b>2</b></Add></soap:Body></soap:Envelope>"
)


def soap11_test():
    """SOAP 1.1 call (``text/xml`` + ``SOAPAction`` header)."""
    engine = StrobEngine(
        url=SOAP_URL,
        concurrency=10,
        duration=30,
        options=RequestOptions(
            soap_enabled=True,
            soap_action="Add",
            body=ENVELOPE,
        ),
    )
    summary = engine.run()
    print_summary(summary)


def soap12_test():
    """SOAP 1.2 call (``application/soap+xml`` + ``action=`` parameter)."""
    engine = StrobEngine(
        url=SOAP_URL,
        concurrency=10,
        duration=30,
        options=RequestOptions(
            soap_enabled=True,
            soap_version=SoapVersionEnum.soap12,
            soap_action="Add",
            body=ENVELOPE,
        ),
    )
    summary = engine.run()
    print_summary(summary)


def custom_headers_test():
    """SOAP call with authentication headers."""
    engine = StrobEngine(
        url=SOAP_URL,
        concurrency=5,
        duration=30,
        options=RequestOptions(
            soap_enabled=True,
            soap_action="Add",
            body=ENVELOPE,
            headers=[("Authorization", "Bearer token123")],
        ),
    )
    summary = engine.run()
    print_summary(summary)


def chaos_test():
    """SOAP call with chaos fault injection."""
    engine = StrobEngine(
        url=SOAP_URL,
        concurrency=10,
        duration=30,
        options=RequestOptions(soap_enabled=True, body=ENVELOPE, chaos=True),
    )
    summary = engine.run()
    print_summary(summary)


def result_analysis():
    """Inspect SOAP operation and fault metrics."""
    engine = StrobEngine(
        url=SOAP_URL,
        concurrency=5,
        duration=5,
        options=RequestOptions(
            soap_enabled=True,
            soap_action="Add",
            body=ENVELOPE,
        ),
    )
    summary = engine.run()

    print(f"Total requests:      {summary.total_requests}")
    print(f"Total errors:        {summary.total_errors}")
    if summary.soap is not None:
        print(f"Operations:          {summary.soap.operations_total}")
        print(f"Fault responses:     {summary.soap.fault_responses_total}")
        print(f"SOAP version:        {summary.soap.soap_version}")


if __name__ == "__main__":
    soap11_test()
    soap12_test()
    custom_headers_test()
    chaos_test()
    result_analysis()
