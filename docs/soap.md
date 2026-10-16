# SOAP Load Testing

strobengine can benchmark SOAP web services over HTTP. SOAP is an HTTP `POST`
of an XML envelope; strobengine sends the envelope with version-correct headers
and inspects the response for a SOAP `Fault`, which is counted as a logical
error even when the transport reports HTTP 200.

## Supported Features

| Feature | Description |
|---------|-------------|
| SOAP 1.1 | `text/xml` body with the `SOAPAction` HTTP header |
| SOAP 1.2 | `application/soap+xml` body with an optional `action=` parameter |
| XML Envelopes | Raw envelope supplied via `--body` / `RequestOptions(body=...)` |
| Fault Detection | Namespace-agnostic `<Fault>` detection via an XML parser |
| Custom Headers | Metadata / auth injection via `--header` (overrides Content-Type) |
| Chaos Testing | Latency spikes, corrupted payloads, metadata corruption, connection drops |

SOAP requests always use `POST`. The `Content-Type` and `SOAPAction` headers are
set automatically unless you supply your own.

## Python API

```python
from strobengine import StrobEngine, RequestOptions
from strobengine.engine import SoapVersionEnum

ENVELOPE = (
    '<?xml version="1.0"?>'
    '<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">'
    "<soap:Body><Add><a>1</a><b>2</b></Add></soap:Body></soap:Envelope>"
)

engine = StrobEngine(
    url="http://localhost:8080/soap",
    concurrency=10,
    duration=30,
    options=RequestOptions(
        soap_enabled=True,
        soap_action="Add",
        body=ENVELOPE,
        # soap_version=SoapVersionEnum.soap12,  # default is 1.1
    ),
)
summary = engine.run()

print(summary.soap.operations_total)  # SOAP responses observed
print(summary.soap.fault_responses_total)  # responses containing a Fault
print(summary.soap.soap_version)  # "1.1" or "1.2"
```

## CLI

```bash
strobengine load http://localhost:8080/soap \
  --soap \
  --soap-action Add \
  --body '<?xml version="1.0"?><soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/"><soap:Body><Add><a>1</a><b>2</b></Add></soap:Body></soap:Envelope>' \
  -c 10 -d 30
```

`--soap` also works with the `stress` and `spike` subcommands.

## SOAP 1.1 vs 1.2

The version shapes how the operation is addressed:

| Version | `Content-Type` | Operation carried by |
|---------|----------------|----------------------|
| `--soap-version 1.1` (default) | `text/xml; charset=utf-8` | `SOAPAction` HTTP header (`"<action>"`) |
| `--soap-version 1.2` | `application/soap+xml; charset=utf-8` | `action="<action>"` parameter in `Content-Type` |

- SOAP 1.1 always sends `SOAPAction` — an empty quoted string (`""`) when no
  `--soap-action` is given.
- SOAP 1.2 omits the `action=` parameter entirely when no action is supplied
  (the action may instead be embedded in the envelope).
- If you pass your own `Content-Type` or `SOAPAction` header via `--header`,
  strobengine does not override it.

## Fault Semantics

SOAP reports operation failures inside the XML envelope as a `<Fault>` element:

- SOAP 1.1 faults are conventionally returned with HTTP 500.
- SOAP 1.2 faults are commonly returned with **HTTP 200**.

strobengine counts **any** `<Fault>` response toward `total_errors` (so the
error rate reflects failed operations, not just failed transports) while keeping
the HTTP `status_codes` breakdown truthful — a 1.2 fault still appears as a 200
in the status distribution. Detection is prefix-agnostic (`<soap:Fault>`,
`<SOAP-ENV:Fault>`, `<S:Fault>`, or bare `<Fault>`), and malformed or non-XML
responses are treated as ordinary HTTP responses (not counted as SOAP
operations).

The `soap` report section exposes:

| Field | Meaning |
|-------|---------|
| `operations_total` | Responses parsed as SOAP operations |
| `fault_responses_total` | Responses containing a SOAP `Fault` |
| `soap_version` | `1.1` or `1.2` (from `--soap-version`) |

## Notes

- Configuration is rejected when `--soap` is combined with `--form` or
  `--graphql-query`, or when no envelope is supplied via `--body`.
- WSDL-driven operation generation and WS-Security/MTOM attachments are not
  supported; provide the raw XML envelope.
- Responses are parsed up to a bounded prefix (4&nbsp;MB) while the full body is
  always drained to preserve keep-alive.
