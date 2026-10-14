//! Traffic execution: turn a `TestConfiguration` into concurrent virtual users
//! driving the engine's HTTP client.

use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use http::{HeaderMap, HeaderName, HeaderValue, Method};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::chaos::ChaosEngine;
use crate::metrics::RequestMetric;
use crate::protocols::ProtocolEngine;
use crate::protocols::http::HttpEngine;

use super::error::{WorkerError, WorkerResult};
use super::proto::TestConfiguration;

/// A resolved, engine-ready description of one distributed test run.
#[derive(Debug, Clone)]
pub struct ExecutionPlan {
    /// Target URLs, hit round-robin by the virtual users.
    pub targets: Vec<String>,
    /// HTTP method applied to every request.
    pub method: Method,
    /// Optional request body (first non-empty payload template).
    pub body: Option<Bytes>,
    /// Default headers applied to every request.
    pub headers: HeaderMap,
    /// Per-request timeout.
    pub timeout: Duration,
    /// Number of concurrent virtual users.
    pub virtual_users: usize,
    /// Run duration; `None` means run until stopped.
    pub duration: Option<Duration>,
}

impl ExecutionPlan {
    /// Build a plan from a wire `TestConfiguration`, clamping the VU count to
    /// the worker's advertised ceiling.
    pub fn from_proto(
        config: &TestConfiguration,
        max_vus: u32,
        default_timeout: Duration,
    ) -> WorkerResult<Self> {
        let targets: Vec<String> = config
            .targets
            .iter()
            .map(|t| t.url.trim().to_string())
            .filter(|u| !u.is_empty())
            .collect();
        if targets.is_empty() {
            return Err(WorkerError::Config(
                "test configuration has no target endpoints".into(),
            ));
        }

        let method = if config.http_method.trim().is_empty() {
            Method::GET
        } else {
            Method::from_bytes(config.http_method.trim().as_bytes())
                .map_err(|e| WorkerError::Config(format!("invalid HTTP method: {e}")))?
        };

        let mut headers = HeaderMap::new();
        for header in &config.headers {
            let name = HeaderName::from_bytes(header.name.trim().as_bytes())
                .map_err(|e| WorkerError::Config(format!("invalid header name: {e}")))?;
            let value = HeaderValue::from_str(&header.value)
                .map_err(|e| WorkerError::Config(format!("invalid header value: {e}")))?;
            headers.append(name, value);
        }

        // First non-empty payload template provides the request body.
        let body = config
            .payloads
            .iter()
            .find(|p| !p.body.is_empty())
            .map(|p| {
                if !p.content_type.is_empty() {
                    headers
                        .entry(http::header::CONTENT_TYPE)
                        .or_insert_with(|| {
                            HeaderValue::from_str(&p.content_type)
                                .unwrap_or(HeaderValue::from_static("application/octet-stream"))
                        });
                }
                Bytes::from(p.body.clone())
            });

        let requested_vus = config.virtual_users.max(1) as usize;
        let ceiling = if max_vus == 0 {
            requested_vus
        } else {
            max_vus as usize
        };

        let timeout = if config.request_timeout_ms > 0 {
            Duration::from_millis(config.request_timeout_ms as u64)
        } else {
            default_timeout
        };

        let duration = config.duration.as_ref().and_then(|d| {
            let secs = d.seconds.max(0) as u64;
            let nanos = d.nanos.max(0) as u32;
            if secs == 0 && nanos == 0 {
                None
            } else {
                Some(Duration::new(secs, nanos))
            }
        });

        Ok(Self {
            targets,
            method,
            body,
            headers,
            timeout,
            virtual_users: requested_vus.min(ceiling).max(1),
            duration,
        })
    }

    /// Build the shared protocol engine for this plan.
    pub fn build_engine(&self) -> WorkerResult<Arc<dyn ProtocolEngine>> {
        let client = reqwest::Client::builder()
            .timeout(self.timeout)
            .default_headers(self.headers.clone())
            .build()
            .map_err(|e| WorkerError::Config(format!("failed to build HTTP client: {e}")))?;

        Ok(Arc::new(
            HttpEngine::new()
                .with_client(client)
                .with_method(self.method.clone())
                .with_body(self.body.clone())
                .with_chaos(ChaosEngine::default()),
        ))
    }
}

/// Run the plan's virtual users until the duration elapses or `shutdown` fires.
///
/// Each iteration's [`RequestMetric`] is forwarded to `metric_tx`; the sender is
/// dropped when all VUs have finished, unblocking the collector.
pub async fn run_virtual_users(
    plan: ExecutionPlan,
    shutdown: CancellationToken,
    metric_tx: mpsc::Sender<RequestMetric>,
) -> WorkerResult<()> {
    let engine = plan.build_engine()?;
    let start = Instant::now();
    let mut handles = Vec::with_capacity(plan.virtual_users);

    for vu in 0..plan.virtual_users {
        let engine = Arc::clone(&engine);
        let targets = plan.targets.clone();
        let duration = plan.duration;
        let shutdown = shutdown.clone();
        let metric_tx = metric_tx.clone();
        handles.push(tokio::spawn(async move {
            let mut index = vu;
            loop {
                if shutdown.is_cancelled() {
                    break;
                }
                if let Some(limit) = duration
                    && start.elapsed() >= limit
                {
                    break;
                }
                let url = &targets[index % targets.len()];
                index = index.wrapping_add(1);
                let metric = engine.execute_iteration(url).await;
                if metric_tx.send(metric).await.is_err() {
                    break;
                }
            }
        }));
    }
    drop(metric_tx);

    for handle in handles {
        if let Err(e) = handle.await {
            tracing::warn!(error = %e, "virtual user task panicked");
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::distributed::proto::{Header, PayloadTemplate, TargetEndpoint};

    fn config() -> TestConfiguration {
        TestConfiguration {
            test_run_id: "run-1".into(),
            targets: vec![
                TargetEndpoint {
                    url: "http://127.0.0.1:1/a".into(),
                    ..Default::default()
                },
                TargetEndpoint {
                    url: "http://127.0.0.1:1/b".into(),
                    ..Default::default()
                },
            ],
            http_method: "POST".into(),
            headers: vec![Header {
                name: "X-Test".into(),
                value: "1".into(),
            }],
            payloads: vec![PayloadTemplate {
                content_type: "application/json".into(),
                body: b"{}".to_vec(),
                ..Default::default()
            }],
            virtual_users: 500,
            request_timeout_ms: 250,
            ..Default::default()
        }
    }

    #[test]
    fn builds_plan_and_clamps_vus() {
        let plan = ExecutionPlan::from_proto(&config(), 10, Duration::from_secs(5)).unwrap();
        assert_eq!(plan.targets.len(), 2);
        assert_eq!(plan.method, Method::POST);
        assert_eq!(plan.virtual_users, 10); // clamped to ceiling
        assert_eq!(plan.timeout, Duration::from_millis(250));
        assert_eq!(plan.headers.get("X-Test").unwrap(), "1");
        assert_eq!(plan.body.as_deref(), Some(&b"{}"[..]));
    }

    #[test]
    fn rejects_config_without_targets() {
        let mut cfg = config();
        cfg.targets.clear();
        let err = ExecutionPlan::from_proto(&cfg, 10, Duration::from_secs(1)).unwrap_err();
        assert!(matches!(err, WorkerError::Config(_)));
    }

    #[test]
    fn duration_none_when_zero() {
        let cfg = TestConfiguration {
            targets: vec![TargetEndpoint {
                url: "http://x".into(),
                ..Default::default()
            }],
            ..Default::default()
        };
        let plan = ExecutionPlan::from_proto(&cfg, 10, Duration::from_secs(1)).unwrap();
        assert!(plan.duration.is_none());
        assert_eq!(plan.method, Method::GET);
    }
}
