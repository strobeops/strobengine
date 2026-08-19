use futures_util::StreamExt;
use prost::Message;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Endpoint;
use tonic_reflection::pb::v1::server_reflection_client::ServerReflectionClient;
use tonic_reflection::pb::v1::{
    ServerReflectionRequest, server_reflection_request::MessageRequest,
};

use crate::protocols::grpc_parser::{ProtoError, ProtoSchema};

/// Fetch gRPC service schema via server reflection.
/// Tries v1 first, falls back to v1alpha.
pub async fn fetch_schema_via_reflection(
    endpoint: Endpoint,
    service: &str,
    method: &str,
) -> Result<ProtoSchema, ProtoError> {
    // Try v1 first
    match try_reflection(
        endpoint.clone(),
        service,
        method,
        "grpc.reflection.v1.ServerReflection",
    )
    .await
    {
        Ok(schema) => Ok(schema),
        Err(_) => {
            tracing::debug!("v1 reflection failed, trying v1alpha");
            try_reflection(
                endpoint,
                service,
                method,
                "grpc.reflection.v1alpha.ServerReflection",
            )
            .await
        }
    }
}

async fn try_reflection(
    endpoint: Endpoint,
    service: &str,
    method: &str,
    _reflection_service: &str,
) -> Result<ProtoSchema, ProtoError> {
    let channel = endpoint
        .connect()
        .await
        .map_err(|e| ProtoError::ConnectionError(format!("failed to connect: {}", e)))?;

    let mut client = ServerReflectionClient::new(channel);

    // Send ListServices request to discover services
    let (tx, rx) = mpsc::channel(1);
    tx.send(ServerReflectionRequest {
        host: String::new(),
        message_request: Some(MessageRequest::ListServices(String::new())),
    })
    .await
    .map_err(|e| ProtoError::ReflectionError(format!("failed to send request: {}", e)))?;

    let response = client
        .server_reflection_info(ReceiverStream::new(rx))
        .await
        .map_err(|e| ProtoError::ReflectionError(format!("reflection call failed: {}", e)))?;

    let mut streaming = response.into_inner();

    // Read responses until we get file descriptors
    while let Some(result) = streaming.next().await {
        match result {
            Ok(resp) => {
                if let Some(tonic_reflection::pb::v1::server_reflection_response::MessageResponse::FileDescriptorResponse(fd_resp)) =
                    resp.message_response
                {
                    // Build pool from ALL received file descriptors
                    let mut pool = prost_reflect::DescriptorPool::new();
                    let mut fd_set = prost_types::FileDescriptorSet {
                        file: Vec::new(),
                    };
                    for fd_bytes in &fd_resp.file_descriptor_proto {
                        let fd = prost_types::FileDescriptorProto::decode(fd_bytes.as_slice())
                            .map_err(|e| ProtoError::CompilationError(e.to_string()))?;
                        fd_set.file.push(fd);
                    }
                    pool.add_file_descriptor_set(fd_set)
                        .map_err(|e| ProtoError::CompilationError(e.to_string()))?;

                    // Find service
                    let service_desc = pool
                        .get_service_by_name(service)
                        .or_else(|| {
                            pool.services()
                                .find(|s| s.name().ends_with(&format!(".{}", service)))
                        })
                        .ok_or_else(|| ProtoError::ServiceNotFound(service.to_string()))?;

                    // Find method
                    let clean_method = method
                        .trim_start_matches('/')
                        .split('/')
                        .next_back()
                        .unwrap_or(method);

                    let method_desc = service_desc
                        .methods()
                        .find(|m| m.name() == clean_method)
                        .ok_or_else(|| ProtoError::MethodNotFound(clean_method.to_string()))?;

                    let input_binding = method_desc.input();
                    let input_name = input_binding.full_name();
                    let _message_desc = pool
                        .get_message_by_name(input_name)
                        .ok_or_else(|| ProtoError::MessageNotFound(input_name.to_string()))?;

                    return Ok(ProtoSchema::from_parts(
                        pool,
                        service_desc.name().to_string(),
                        clean_method.to_string(),
                        input_name.to_string(),
                    ));
                }
            }
            Err(e) => {
                return Err(ProtoError::ReflectionError(format!(
                    "reflection response error: {}",
                    e
                )));
            }
        }
    }

    Err(ProtoError::ReflectionError(
        "no file descriptors received from server".into(),
    ))
}
