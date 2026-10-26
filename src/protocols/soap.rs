//! SOAP-over-HTTP engine.
//!
//! SOAP is an HTTP `POST` of an XML envelope. This engine sends the envelope
//! with version-correct headers, drains the response body, and inspects it for a
//! SOAP `Fault` element — counted as a logical error while the real HTTP status
//! stays truthful (SOAP 1.2 faults commonly arrive on HTTP 200).

use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::Bytes;
use futures_util::StreamExt;
use http::Method;
use quick_xml::events::Event;

use crate::chaos::{ChaosEngine, ChaosFault};
use crate::config::SoapVersion;
use crate::metrics::RequestMetric;

use super::ProtocolEngine;

static CORRUPTED_BODY: &[u8] = b"<Envelope><Body><Corrupted/></Body></Envelope>";
static CHAOS_HEADER: &str = "x-chaos-fault";
static BAD_HEADER_VALUE: &str = "invalid-header-value";

/// Cap on how much of a response body is buffered for XML parsing. Larger
/// bodies are still fully drained (to preserve keep-alive) but only the first
/// `MAX_PARSE_BYTES` are inspected.
const MAX_PARSE_BYTES: usize = 4 * 1024 * 1024;

/// The `Content-Type` and optional `SOAPAction` header for a SOAP request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SoapRequestHeaders {
    /// Value for the `Content-Type` header.
    pub content_type: String,
    /// Value for the `SOAPAction` header (SOAP 1.1 only).
    pub soap_action: Option<String>,
}

/// Compute the SOAP headers for `version` and an optional operation action.
///
/// - SOAP 1.1 → `text/xml; charset=utf-8` plus a `SOAPAction` header (an empty
///   quoted string when no action is supplied, as 1.1 requires the header).
/// - SOAP 1.2 → `application/soap+xml; charset=utf-8`, with the action carried
///   as an `action="..."` parameter when present (and no `SOAPAction` header).
pub fn request_headers(version: SoapVersion, action: Option<&str>) -> SoapRequestHeaders {
    match version {
        SoapVersion::Soap11 => SoapRequestHeaders {
            content_type: "text/xml; charset=utf-8".to_string(),
            soap_action: Some(action.unwrap_or("").to_string()),
        },
        SoapVersion::Soap12 => {
            let content_type = match action {
                Some(action) if !action.is_empty() => {
                    format!("application/soap+xml; charset=utf-8; action=\"{action}\"")
                }
                _ => "application/soap+xml; charset=utf-8".to_string(),
            };
            SoapRequestHeaders {
                content_type,
                soap_action: None,
            }
        }
    }
}

/// Parse a SOAP response body, returning `Some(1)` when it contains a `Fault`
/// element and `Some(0)` for a well-formed non-fault envelope.
///
/// Detection is namespace/prefix-agnostic (matches `<soap:Fault>`,
/// `<SOAP-ENV:Fault>`, `<S:Fault>` or bare `<Fault>`). Bodies that are not XML
/// (no elements) or are malformed return `None`, so non-SOAP responses are not
/// misclassified. Never panics on malformed input.
pub fn count_soap_faults(body: &[u8]) -> Option<u64> {
    let mut reader = quick_xml::Reader::from_reader(body);
    let mut buf = Vec::new();
    let mut saw_element = false;
    loop {
        match reader.read_event_into(&mut buf) {
            Ok(Event::Start(element)) | Ok(Event::Empty(element)) => {
                saw_element = true;
                if element.local_name().as_ref() == "Fault" {
                    return Some(1);
                }
            }
            Ok(Event::Eof) => return if saw_element { Some(0) } else { None },
            Ok(_) => {}
            Err(_) => return None,
        }
        buf.clear();
    }
}

pub struct SoapEngine {
    client: reqwest::Client,
    payload: Bytes,
    chaos: ChaosEngine,
}

impl SoapEngine {
    pub fn new(client: reqwest::Client, payload: Bytes, chaos: ChaosEngine) -> Self {
        Self {
            client,
            payload,
            chaos,
        }
    }
}

/// Read the response body to EOF, counting bytes received and buffering a
/// bounded prefix for XML parsing. Draining fully returns the socket to the
/// reqwest pool (see `http.rs`).
async fn drain_body(response: reqwest::Response) -> (u64, Option<Vec<u8>>, Option<reqwest::Error>) {
    let mut stream = response.bytes_stream();
    let mut total_bytes = 0u64;
    let mut buffered: Vec<u8> = Vec::new();
    while let Some(chunk) = stream.next().await {
        match chunk {
            Ok(chunk) => {
                total_bytes += chunk.len() as u64;
                if buffered.len() < MAX_PARSE_BYTES {
                    let remaining = MAX_PARSE_BYTES - buffered.len();
                    buffered.extend_from_slice(&chunk[..chunk.len().min(remaining)]);
                }
            }
            Err(e) => return (total_bytes, None, Some(e)),
        }
    }
    (total_bytes, Some(buffered), None)
}

#[async_trait]
impl ProtocolEngine for SoapEngine {
    async fn execute_iteration(&self, target_url: &str) -> RequestMetric {
        let req_start = Instant::now();

        let base_request = || {
            self.client
                .request(Method::POST, target_url)
                .body(self.payload.clone())
        };

        let chaos_fault = self.chaos.select_fault();
        let request = match chaos_fault {
            Some(ChaosFault::LatencySpike { duration_ms }) => {
                tracing::trace!(duration_ms, "chaos: latency spike injected");
                tokio::time::sleep(Duration::from_millis(duration_ms)).await;
                base_request()
            }
            Some(ChaosFault::CorruptedPayload) => {
                tracing::trace!("chaos: corrupted payload injected");
                self.client
                    .request(Method::POST, target_url)
                    .header(CHAOS_HEADER, "corrupted-payload")
                    .body(CORRUPTED_BODY)
            }
            Some(ChaosFault::MetadataCorruption) => {
                tracing::trace!("chaos: metadata corruption injected");
                base_request().header(CHAOS_HEADER, BAD_HEADER_VALUE)
            }
            Some(ChaosFault::ConnectionDrop) => {
                tracing::trace!("chaos: connection drop injected");
                base_request().timeout(Duration::from_nanos(1))
            }
            None => base_request(),
        };

        let metric = match request.send().await {
            Ok(response) => {
                let status_code = response.status().as_u16();
                let (bytes_received, body, body_error) = drain_body(response).await;
                if let Some(ref e) = body_error {
                    tracing::debug!(error = %e, bytes_received, "response body read failed");
                    RequestMetric::builder(req_start.elapsed().as_micros(), chaos_fault)
                        .status(0)
                        .bytes(bytes_received)
                        .build()
                } else {
                    let soap_errors = body.as_deref().and_then(count_soap_faults);
                    RequestMetric::builder(req_start.elapsed().as_micros(), chaos_fault)
                        .status(status_code)
                        .bytes(bytes_received)
                        .soap(soap_errors)
                        .build()
                }
            }
            Err(e) => {
                tracing::debug!(error = %e, "request failed");
                RequestMetric::error(req_start.elapsed().as_micros(), chaos_fault)
            }
        };

        if tracing::enabled!(tracing::Level::TRACE) {
            tracing::trace!(
                status = metric.status_code,
                latency_us = metric.latency_micros,
                soap_errors = ?metric.soap().map(|s| s.faults),
                "soap request completed"
            );
        }

        metric
    }
}

#[cfg(test)]
mod tests {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    use super::*;

    #[test]
    fn soap11_headers_use_soapaction_header() {
        let headers = request_headers(SoapVersion::Soap11, Some("Add"));
        assert_eq!(headers.content_type, "text/xml; charset=utf-8");
        assert_eq!(headers.soap_action.as_deref(), Some("Add"));
    }

    #[test]
    fn soap11_without_action_sends_empty_soapaction() {
        let headers = request_headers(SoapVersion::Soap11, None);
        assert_eq!(headers.soap_action.as_deref(), Some(""));
    }

    #[test]
    fn soap12_with_action_embeds_action_parameter() {
        let headers = request_headers(SoapVersion::Soap12, Some("Add"));
        assert_eq!(
            headers.content_type,
            "application/soap+xml; charset=utf-8; action=\"Add\""
        );
        assert!(headers.soap_action.is_none());
    }

    #[test]
    fn soap12_without_action_omits_action_parameter() {
        for action in [None, Some("")] {
            let headers = request_headers(SoapVersion::Soap12, action);
            assert_eq!(headers.content_type, "application/soap+xml; charset=utf-8");
            assert!(!headers.content_type.ends_with(';'));
        }
    }

    #[test]
    fn detects_fault_with_any_prefix() {
        for body in [
            br#"<soap:Envelope xmlns:soap="x"><soap:Body><soap:Fault><faultcode>soap:Server</faultcode></soap:Fault></soap:Body></soap:Envelope>"#.as_slice(),
            br#"<S:Envelope xmlns:S="x"><S:Body><S:Fault><Code><Value>S:Receiver</Value></Code></S:Fault></S:Body></S:Envelope>"#.as_slice(),
            br#"<Envelope><Body><Fault/></Body></Envelope>"#.as_slice(),
        ] {
            assert_eq!(count_soap_faults(body), Some(1));
        }
    }

    #[test]
    fn valid_non_fault_envelope_is_zero() {
        let body = br#"<soap:Envelope xmlns:soap="x"><soap:Body><AddResponse><result>3</result></AddResponse></soap:Body></soap:Envelope>"#;
        assert_eq!(count_soap_faults(body), Some(0));
    }

    #[test]
    fn non_xml_and_malformed_are_none() {
        // No elements at all -> not a SOAP response.
        assert_eq!(count_soap_faults(b"Internal Server Error"), None);
        assert_eq!(count_soap_faults(b""), None);
        // A mismatched end tag is rejected by the parser.
        assert_eq!(count_soap_faults(b"<a></b>"), None);
    }

    #[test]
    fn leniently_parsed_non_fault_xml_is_zero() {
        // quick-xml tolerates unclosed tags; the body is still not a Fault.
        assert_eq!(count_soap_faults(b"<not><closed>"), Some(0));
    }

    /// Raw local server returning a canned XML response.
    async fn spawn_server(body: &'static str, status: u16) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            if let Ok((mut socket, _)) = listener.accept().await {
                let mut buf = [0u8; 4096];
                let mut seen = 0usize;
                while !buf[..seen].windows(4).any(|w| w == b"\r\n\r\n") {
                    match socket.read(&mut buf[seen..]).await {
                        Ok(0) | Err(_) => return,
                        Ok(n) => seen += n,
                    }
                }
                let response = format!(
                    "HTTP/1.1 {status} OK\r\nContent-Type: text/xml\r\nContent-Length: {}\r\n\r\n{}",
                    body.len(),
                    body
                );
                let _ = socket.write_all(response.as_bytes()).await;
                let _ = socket.flush().await;
            }
        });
        format!("http://{addr}")
    }

    fn engine() -> SoapEngine {
        SoapEngine::new(
            reqwest::Client::new(),
            Bytes::from_static(b"<Envelope><Body/></Envelope>"),
            ChaosEngine::default(),
        )
    }

    #[tokio::test]
    async fn iteration_reports_fault_on_http_200() {
        let url = spawn_server(
            r#"<soap:Envelope xmlns:soap="x"><soap:Body><soap:Fault/></soap:Body></soap:Envelope>"#,
            200,
        )
        .await;
        let metric = engine().execute_iteration(&url).await;
        assert_eq!(metric.status_code, 200);
        assert_eq!(metric.soap().map(|s| s.faults), Some(1));
    }

    #[tokio::test]
    async fn iteration_reports_zero_for_valid_response() {
        let url = spawn_server(
            r#"<soap:Envelope xmlns:soap="x"><soap:Body><ok/></soap:Body></soap:Envelope>"#,
            200,
        )
        .await;
        let metric = engine().execute_iteration(&url).await;
        assert_eq!(metric.status_code, 200);
        assert_eq!(metric.soap().map(|s| s.faults), Some(0));
    }
}
