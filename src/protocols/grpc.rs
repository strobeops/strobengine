use std::time::{Duration, Instant};

use async_trait::async_trait;
use base64::Engine;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use tonic::Status;
use tonic::client::Grpc;
use tonic::codec::{Codec, DecodeBuf, Encoder};
use tonic::transport::Endpoint;

use crate::chaos::{ChaosEngine, ChaosFault};
use crate::metrics::RequestMetric;

use super::ProtocolEngine;

/// Maps gRPC status codes to HTTP-equivalent status codes.
fn grpc_to_http_status(code: u16) -> u16 {
    match code {
        0 => 200,  // OK
        1 => 499,  // Cancelled
        2 => 500,  // Unknown
        3 => 400,  // InvalidArgument
        4 => 504,  // DeadlineExceeded
        5 => 404,  // NotFound
        6 => 409,  // AlreadyExists
        7 => 403,  // PermissionDenied
        8 => 429,  // ResourceExhausted
        9 => 412,  // FailedPrecondition
        10 => 409, // Aborted
        11 => 416, // OutOfRange
        12 => 501, // Unimplemented
        13 => 500, // Internal
        14 => 503, // Unavailable
        15 => 500, // DataLoss
        16 => 401, // Unauthenticated
        _ => 500,  // Unknown codes
    }
}

/// A pass-through codec for raw protobuf bytes.
struct RawCodec;

impl Codec for RawCodec {
    type Encode = Bytes;
    type Decode = Bytes;
    type Encoder = RawEncoder;
    type Decoder = RawDecoder;

    fn encoder(&mut self) -> Self::Encoder {
        RawEncoder
    }

    fn decoder(&mut self) -> Self::Decoder {
        RawDecoder
    }
}

struct RawEncoder;

impl Encoder for RawEncoder {
    type Item = Bytes;
    type Error = Status;

    fn encode(
        &mut self,
        item: Self::Item,
        dst: &mut tonic::codec::EncodeBuf<'_>,
    ) -> Result<(), Self::Error> {
        dst.put(item);
        Ok(())
    }
}

struct RawDecoder;

impl tonic::codec::Decoder for RawDecoder {
    type Item = Bytes;
    type Error = Status;

    fn decode(&mut self, src: &mut DecodeBuf<'_>) -> Result<Option<Self::Item>, Self::Error> {
        let len = src.remaining();
        if len == 0 {
            return Ok(None);
        }
        let mut buf = BytesMut::with_capacity(len);
        buf.extend_from_slice(src.chunk());
        src.advance(len);
        Ok(Some(buf.freeze()))
    }
}

pub struct GrpcEngine {
    endpoint: Endpoint,
    #[allow(dead_code)] // Will be used for gRPC metadata in future
    headers: Vec<(String, String)>,
    chaos: ChaosEngine,
    service: String,
    method: String,
    payload: Vec<u8>,
    deadline_ms: Option<u64>,
    #[allow(dead_code)] // Stored for future use (e.g., dynamic response decoding)
    proto_schema: Option<crate::protocols::grpc_parser::ProtoSchema>,
}

impl GrpcEngine {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        url: &str,
        headers: Vec<(String, String)>,
        chaos: ChaosEngine,
        service: Option<String>,
        method: Option<String>,
        grpc_payload: Option<String>,
        deadline_ms: Option<u64>,
        proto_path: Option<String>,
    ) -> Result<Self, crate::protocols::grpc_parser::ProtoError> {
        // Parse the URL directly - tonic handles scheme normalization
        let uri: http::Uri = url.parse().expect("invalid gRPC endpoint URL");

        let endpoint = Endpoint::new(uri).expect("failed to create gRPC endpoint");

        let svc = service.clone().unwrap_or_default();
        let mth = method.clone().unwrap_or_default();

        // If proto_path is provided, parse schema and convert JSON to protobuf
        let (payload, proto_schema) = if let (Some(path), Some(json)) = (&proto_path, &grpc_payload)
        {
            let schema = crate::protocols::grpc_parser::ProtoSchema::new(path, &svc, &mth)?;
            let bytes = schema.json_to_protobuf(json)?;
            tracing::info!(
                proto_path = %path,
                service = %svc,
                method = %mth,
                payload_bytes = bytes.len(),
                "JSON payload converted to protobuf"
            );
            (bytes, Some(schema))
        } else {
            // Decode payload: try hex (0x prefix) first, then base64
            let payload = grpc_payload
                .as_deref()
                .map(|s| {
                    if let Some(hex_str) = s.strip_prefix("0x") {
                        hex::decode(hex_str).unwrap_or_default()
                    } else {
                        base64::engine::general_purpose::STANDARD
                            .decode(s)
                            .unwrap_or_default()
                    }
                })
                .unwrap_or_default();
            (payload, None)
        };

        Ok(Self {
            endpoint,
            headers,
            chaos,
            service: svc,
            method: mth,
            payload,
            deadline_ms,
            proto_schema,
        })
    }
}

#[async_trait]
impl ProtocolEngine for GrpcEngine {
    async fn execute_iteration(&self, _target_url: &str) -> RequestMetric {
        let req_start = Instant::now();

        // Phase 1: Pre-connection chaos
        let fault = self.chaos.select_fault();

        // ConnectionDrop: short-circuit with timeout
        if let Some(ChaosFault::ConnectionDrop) = fault {
            tracing::trace!("grpc chaos: connection drop");
            let _ = tokio::time::timeout(Duration::from_nanos(1), self.endpoint.connect()).await;
            return RequestMetric {
                latency_micros: req_start.elapsed().as_micros(),
                status_code: 0,
                bytes_received: 0,
            };
        }

        // LatencySpike: sleep before connecting
        if let Some(ChaosFault::LatencySpike { duration_ms }) = fault {
            tracing::trace!(duration_ms, "grpc chaos: latency spike");
            tokio::time::sleep(Duration::from_millis(duration_ms)).await;
        }

        // Connect to gRPC server
        let channel = match self.endpoint.connect().await {
            Ok(ch) => ch,
            Err(e) => {
                tracing::debug!(error = %e, "gRPC connection failed");
                return RequestMetric {
                    latency_micros: req_start.elapsed().as_micros(),
                    status_code: 0,
                    bytes_received: 0,
                };
            }
        };

        // Phase 2: Post-connection chaos
        let payload = match fault {
            Some(ChaosFault::CorruptedPayload) => {
                tracing::trace!("grpc chaos: corrupted payload");
                Bytes::from_static(b"\xff\xfe\xbd\xef")
            }
            Some(ChaosFault::MetadataCorruption) => {
                tracing::trace!("grpc chaos: metadata corruption");
                Bytes::from(self.payload.clone())
            }
            _ => Bytes::from(self.payload.clone()),
        };

        // Build gRPC path: /service/method
        let path = match http::uri::PathAndQuery::from_maybe_shared(format!(
            "/{}/{}",
            self.service, self.method
        )) {
            Ok(p) => p,
            Err(e) => {
                tracing::debug!(error = %e, "invalid gRPC path");
                return RequestMetric {
                    latency_micros: req_start.elapsed().as_micros(),
                    status_code: 0,
                    bytes_received: 0,
                };
            }
        };

        // Build gRPC client
        let mut client = Grpc::new(channel);

        // Build request with optional metadata corruption
        let mut request = tonic::Request::new(payload);
        if let Some(ChaosFault::MetadataCorruption) = fault {
            request.metadata_mut().insert(
                "x-chaos-fault",
                tonic::metadata::MetadataValue::from_static("corrupted_value"),
            );
        }

        // Make unary gRPC call with optional deadline
        let call = async { client.unary(request, path, RawCodec).await };

        let response = if let Some(deadline_ms) = self.deadline_ms {
            match tokio::time::timeout(Duration::from_millis(deadline_ms), call).await {
                Ok(result) => result,
                Err(_) => {
                    tracing::debug!("gRPC call timed out");
                    return RequestMetric {
                        latency_micros: req_start.elapsed().as_micros(),
                        status_code: 0,
                        bytes_received: 0,
                    };
                }
            }
        } else {
            call.await
        };

        let (status_code, bytes_received) = match response {
            Ok(resp) => {
                let grpc_status = resp
                    .metadata()
                    .get("grpc-status")
                    .and_then(|v| v.to_str().ok())
                    .and_then(|s| s.parse::<u16>().ok())
                    .unwrap_or(0);
                let code = grpc_to_http_status(grpc_status);
                let bytes = resp.into_inner().len() as u64;
                (code, bytes)
            }
            Err(e) => {
                tracing::debug!(error = %e, "gRPC call failed");
                (0, 0)
            }
        };

        let latency_micros = req_start.elapsed().as_micros();

        if tracing::enabled!(tracing::Level::TRACE) {
            tracing::trace!(
                status = status_code,
                latency_us = latency_micros,
                "gRPC iteration completed"
            );
        }

        RequestMetric {
            latency_micros,
            status_code,
            bytes_received,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_grpc_to_http_status() {
        assert_eq!(grpc_to_http_status(0), 200);
        assert_eq!(grpc_to_http_status(3), 400);
        assert_eq!(grpc_to_http_status(13), 500);
        assert_eq!(grpc_to_http_status(14), 503);
        assert_eq!(grpc_to_http_status(16), 401);
    }

    #[test]
    fn test_grpc_url_normalization() {
        let engine = GrpcEngine::new(
            "grpc://127.0.0.1:50051",
            vec![],
            ChaosEngine::default(),
            Some("pkg.Svc".into()),
            Some("Method".into()),
            None,
            None,
            None,
        )
        .unwrap();
        // tonic accepts grpc:// scheme directly
        let uri_str = engine.endpoint.uri().to_string();
        assert!(uri_str.contains("127.0.0.1:50051"));

        let engine = GrpcEngine::new(
            "grpcs://127.0.0.1:50051",
            vec![],
            ChaosEngine::default(),
            None,
            None,
            None,
            None,
            None,
        )
        .unwrap();
        let uri_str = engine.endpoint.uri().to_string();
        assert!(uri_str.contains("127.0.0.1:50051"));
    }

    #[test]
    fn test_base64_payload_decoding() {
        let engine = GrpcEngine::new(
            "grpc://127.0.0.1:50051",
            vec![],
            ChaosEngine::default(),
            None,
            None,
            Some("dGVzdA==".into()), // base64 for "test"
            None,
            None,
        )
        .unwrap();
        assert_eq!(engine.payload, b"test");
    }

    #[test]
    fn test_empty_payload_on_invalid_base64() {
        let engine = GrpcEngine::new(
            "grpc://127.0.0.1:50051",
            vec![],
            ChaosEngine::default(),
            None,
            None,
            Some("not-valid-base64!!!".into()),
            None,
            None,
        )
        .unwrap();
        assert!(engine.payload.is_empty());
    }

    #[test]
    fn test_hex_payload_decoding() {
        let engine = GrpcEngine::new(
            "grpc://127.0.0.1:50051",
            vec![],
            ChaosEngine::default(),
            None,
            None,
            Some("0x0801".into()), // hex for protobuf varint 1
            None,
            None,
        )
        .unwrap();
        assert_eq!(engine.payload, vec![0x08, 0x01]);
    }

    #[test]
    fn test_hex_payload_deadbeef() {
        let engine = GrpcEngine::new(
            "grpc://127.0.0.1:50051",
            vec![],
            ChaosEngine::default(),
            None,
            None,
            Some("0xdeadbeef".into()),
            None,
            None,
        )
        .unwrap();
        assert_eq!(engine.payload, vec![0xde, 0xad, 0xbe, 0xef]);
    }
}
