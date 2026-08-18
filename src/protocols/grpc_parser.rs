use prost::Message;
use prost_reflect::{DescriptorPool, DynamicMessage};

/// Errors that can occur during proto parsing or JSON conversion.
#[derive(Debug)]
pub enum ProtoError {
    CompilationError(String),
    ServiceNotFound(String),
    MethodNotFound(String),
    MessageNotFound(String),
    JsonError(String),
    EncodeError(String),
}

impl std::fmt::Display for ProtoError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::CompilationError(e) => write!(f, "proto compilation failed: {}", e),
            Self::ServiceNotFound(s) => write!(f, "service not found: {}", s),
            Self::MethodNotFound(m) => write!(f, "method not found: {}", m),
            Self::MessageNotFound(m) => write!(f, "message not found: {}", m),
            Self::JsonError(e) => write!(f, "JSON error: {}", e),
            Self::EncodeError(e) => write!(f, "protobuf encode error: {}", e),
        }
    }
}

impl std::error::Error for ProtoError {}

/// Parsed .proto schema with service, method, and message descriptors.
pub struct ProtoSchema {
    #[allow(dead_code)]
    pool: DescriptorPool,
    service_name: String,
    method_name: String,
    message_name: String,
}

impl ProtoSchema {
    /// Parse a .proto file and extract service/method/message descriptors.
    pub fn new(proto_path: &str, service: &str, method: &str) -> Result<Self, ProtoError> {
        // Add parent directory of proto file as include path
        let proto_dir = std::path::Path::new(proto_path)
            .parent()
            .map(|p| p.to_string_lossy().to_string())
            .unwrap_or_default();

        // Compile .proto file
        let file_descriptor_set = protox::compile([proto_path], [&proto_dir])
            .map_err(|e| ProtoError::CompilationError(e.to_string()))?;

        // Load into DescriptorPool
        let encoded = file_descriptor_set.encode_to_vec();
        let pool = DescriptorPool::decode(&encoded[..])
            .map_err(|e| ProtoError::CompilationError(e.to_string()))?;

        // Try exact match first, then search all services for suffix match
        let service_desc = pool
            .get_service_by_name(service)
            .or_else(|| {
                pool.services()
                    .find(|s| s.name().ends_with(&format!(".{}", service)) || s.name() == service)
            })
            .ok_or_else(|| ProtoError::ServiceNotFound(service.to_string()))?;

        // Strip leading slashes and service prefixes from method name
        let clean_method = method
            .trim_start_matches('/')
            .split('/')
            .next_back()
            .unwrap_or(method);

        // Find method by iterating over methods (method_by_name may not exist)
        let method_desc = service_desc
            .methods()
            .find(|m| m.name() == clean_method)
            .ok_or_else(|| ProtoError::MethodNotFound(clean_method.to_string()))?;

        let input_binding = method_desc.input();
        let input_name = input_binding.full_name();
        let _message_desc = pool
            .get_message_by_name(input_name)
            .ok_or_else(|| ProtoError::MessageNotFound(input_name.to_string()))?;

        Ok(Self {
            pool,
            service_name: service_desc.name().to_string(),
            method_name: clean_method.to_string(),
            message_name: input_name.to_string(),
        })
    }

    /// Get the fully-qualified gRPC path (e.g., "/package.Service/Method")
    pub fn grpc_path(&self) -> String {
        format!("/{}/{}", self.service_name, self.method_name)
    }

    /// Convert a JSON string to protobuf-encoded bytes using the schema.
    pub fn json_to_protobuf(&self, json_str: &str) -> Result<Vec<u8>, ProtoError> {
        let message_desc = self
            .pool
            .get_message_by_name(&self.message_name)
            .ok_or_else(|| ProtoError::MessageNotFound(self.message_name.clone()))?;

        // Deserialize JSON into DynamicMessage using the message descriptor
        let mut deserializer = serde_json::de::Deserializer::from_str(json_str);
        let dynamic_msg = DynamicMessage::deserialize(message_desc, &mut deserializer)
            .map_err(|e| ProtoError::JsonError(e.to_string()))?;

        let mut buf = Vec::new();
        dynamic_msg
            .encode(&mut buf)
            .map_err(|e| ProtoError::EncodeError(e.to_string()))?;
        Ok(buf)
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_strip_method_prefix() {
        let method = "/package.Service/Execute";
        let clean = method
            .trim_start_matches('/')
            .split('/')
            .next_back()
            .unwrap_or(method);
        assert_eq!(clean, "Execute");
    }
}
