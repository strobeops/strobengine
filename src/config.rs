use std::time::Duration;

use crate::chaos::DEFAULT_CHAOS_RATE;
use pyo3::prelude::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[pyclass(from_py_object)]
pub enum WsMode {
    #[default]
    Handshake,
    PingPong,
    Stream,
}

#[pymethods]
impl WsMode {
    #[staticmethod]
    fn handshake() -> Self {
        Self::Handshake
    }

    #[staticmethod]
    fn ping_pong() -> Self {
        Self::PingPong
    }

    #[staticmethod]
    fn stream() -> Self {
        Self::Stream
    }
}

#[pyclass(from_py_object)]
#[derive(Clone)]
pub struct TestConfig {
    #[pyo3(get, set)]
    pub url: String,
    #[pyo3(get, set)]
    pub concurrency: usize,
    #[pyo3(get, set)]
    pub duration_secs: u64,
    #[pyo3(get, set)]
    pub timeout_secs: u64,
    #[pyo3(get, set)]
    pub chaos: bool,
    #[pyo3(get, set)]
    pub chaos_rate: f32,
    #[pyo3(get, set)]
    pub no_progress: bool,
    #[pyo3(get, set)]
    pub method: String,
    #[pyo3(get, set)]
    pub body: Option<String>,
    #[pyo3(get, set)]
    pub form: Option<Vec<(String, String)>>,
    #[pyo3(get, set)]
    pub headers: Option<Vec<(String, String)>>,
    #[pyo3(get, set)]
    pub ws_mode: WsMode,
    #[pyo3(get, set)]
    pub ws_payload: Option<String>,
    #[pyo3(get, set)]
    pub grpc_service: Option<String>,
    #[pyo3(get, set)]
    pub grpc_method: Option<String>,
    #[pyo3(get, set)]
    pub grpc_payload: Option<String>,
    #[pyo3(get, set)]
    pub grpc_deadline_ms: Option<u64>,
    #[pyo3(get, set)]
    pub proto_path: Option<String>,
    #[pyo3(get, set)]
    pub grpc_use_reflection: bool,
    #[pyo3(get, set)]
    pub ws_persistent: bool,
    #[pyo3(get, set)]
    pub ws_keepalive_secs: Option<u64>,
    #[pyo3(get, set)]
    pub ws_max_messages: Option<u64>,
    #[pyo3(get, set)]
    pub ws_role: Option<String>,
    #[pyo3(get, set)]
    pub ws_publish_interval_ms: Option<u64>,
    #[pyo3(get, set)]
    pub ws_subscribers: Option<usize>,
    #[pyo3(get, set)]
    pub http3_enabled: bool,
    #[pyo3(get, set)]
    pub quic_zero_rtt: bool,
    #[pyo3(get, set)]
    pub quic_max_idle_timeout_ms: Option<u64>,
    #[pyo3(get, set)]
    pub sse_enabled: bool,
    #[pyo3(get, set)]
    pub sse_max_events: Option<u64>,
    #[pyo3(get, set)]
    pub output_dir: Option<String>,
    #[pyo3(get, set)]
    pub no_save: bool,
}

#[pymethods]
impl TestConfig {
    #[new]
    #[pyo3(signature = (
        url,
        concurrency=10,
        duration_secs=10,
        timeout_secs=10,
        chaos=false,
        chaos_rate=DEFAULT_CHAOS_RATE,
        no_progress=false,
        method="GET",
        body=None,
        form=None,
        headers=None,
        ws_mode=WsMode::Handshake,
        ws_payload=None,
        grpc_service=None,
        grpc_method=None,
        grpc_payload=None,
        grpc_deadline_ms=None,
        proto_path=None,
        grpc_use_reflection=false,
        ws_persistent=false,
        ws_keepalive_secs=None,
        ws_max_messages=None,
        ws_role=None,
        ws_publish_interval_ms=None,
        ws_subscribers=None,
        http3_enabled=false,
        quic_zero_rtt=false,
        quic_max_idle_timeout_ms=None,
        sse_enabled=false,
        sse_max_events=None,
        output_dir=None,
        no_save=false,
    ))]
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        url: String,
        concurrency: usize,
        duration_secs: u64,
        timeout_secs: u64,
        chaos: bool,
        chaos_rate: f32,
        no_progress: bool,
        method: &str,
        body: Option<String>,
        form: Option<Vec<(String, String)>>,
        headers: Option<Vec<(String, String)>>,
        ws_mode: WsMode,
        ws_payload: Option<String>,
        grpc_service: Option<String>,
        grpc_method: Option<String>,
        grpc_payload: Option<String>,
        grpc_deadline_ms: Option<u64>,
        proto_path: Option<String>,
        grpc_use_reflection: bool,
        ws_persistent: bool,
        ws_keepalive_secs: Option<u64>,
        ws_max_messages: Option<u64>,
        ws_role: Option<String>,
        ws_publish_interval_ms: Option<u64>,
        ws_subscribers: Option<usize>,
        http3_enabled: bool,
        quic_zero_rtt: bool,
        quic_max_idle_timeout_ms: Option<u64>,
        sse_enabled: bool,
        sse_max_events: Option<u64>,
        output_dir: Option<String>,
        no_save: bool,
    ) -> Self {
        Self {
            url,
            concurrency,
            duration_secs,
            timeout_secs,
            chaos,
            chaos_rate,
            no_progress,
            method: method.to_string(),
            body,
            form,
            headers,
            ws_mode,
            ws_payload,
            grpc_service,
            grpc_method,
            grpc_payload,
            grpc_deadline_ms,
            proto_path,
            grpc_use_reflection,
            ws_persistent,
            ws_keepalive_secs,
            ws_max_messages,
            ws_role,
            ws_publish_interval_ms,
            ws_subscribers,
            http3_enabled,
            quic_zero_rtt,
            quic_max_idle_timeout_ms,
            sse_enabled,
            sse_max_events,
            output_dir,
            no_save,
        }
    }
}

impl TestConfig {
    /// Create a builder for programmatic construction (tests, internal factories).
    pub fn builder(url: String) -> TestConfigBuilder {
        TestConfigBuilder::new(url)
    }

    /// Minimal config for protocol detection (used by `run_load_profiles`).
    pub fn for_protocol_detection(
        url: String,
        concurrency: usize,
        duration_secs: u64,
        timeout_secs: u64,
    ) -> Self {
        Self::builder(url)
            .concurrency(concurrency)
            .duration_secs(duration_secs)
            .timeout_secs(timeout_secs)
            .build()
    }
}

/// Builder for `TestConfig` with sane defaults. Used for Rust-internal
/// construction (tests, `for_protocol_detection`) — not exposed to Python.
pub struct TestConfigBuilder {
    url: String,
    concurrency: usize,
    duration_secs: u64,
    timeout_secs: u64,
    chaos: bool,
    chaos_rate: f32,
    no_progress: bool,
    method: String,
    body: Option<String>,
    form: Option<Vec<(String, String)>>,
    headers: Option<Vec<(String, String)>>,
    ws_mode: WsMode,
    ws_payload: Option<String>,
    ws_persistent: bool,
    ws_keepalive_secs: Option<u64>,
    ws_max_messages: Option<u64>,
    ws_role: Option<String>,
    ws_publish_interval_ms: Option<u64>,
    ws_subscribers: Option<usize>,
    grpc_service: Option<String>,
    grpc_method: Option<String>,
    grpc_payload: Option<String>,
    grpc_deadline_ms: Option<u64>,
    proto_path: Option<String>,
    grpc_use_reflection: bool,
    http3_enabled: bool,
    quic_zero_rtt: bool,
    quic_max_idle_timeout_ms: Option<u64>,
    sse_enabled: bool,
    sse_max_events: Option<u64>,
    output_dir: Option<String>,
    no_save: bool,
}

impl Default for TestConfigBuilder {
    fn default() -> Self {
        Self {
            url: String::new(),
            concurrency: 10,
            duration_secs: 10,
            timeout_secs: 10,
            chaos: false,
            chaos_rate: DEFAULT_CHAOS_RATE,
            no_progress: false,
            method: "GET".to_string(),
            body: None,
            form: None,
            headers: None,
            ws_mode: WsMode::Handshake,
            ws_payload: None,
            ws_persistent: false,
            ws_keepalive_secs: None,
            ws_max_messages: None,
            ws_role: None,
            ws_publish_interval_ms: None,
            ws_subscribers: None,
            grpc_service: None,
            grpc_method: None,
            grpc_payload: None,
            grpc_deadline_ms: None,
            proto_path: None,
            grpc_use_reflection: false,
            http3_enabled: false,
            quic_zero_rtt: false,
            quic_max_idle_timeout_ms: None,
            sse_enabled: false,
            sse_max_events: None,
            output_dir: None,
            no_save: false,
        }
    }
}

impl TestConfigBuilder {
    pub fn new(url: String) -> Self {
        Self {
            url,
            ..Default::default()
        }
    }

    pub fn concurrency(mut self, v: usize) -> Self {
        self.concurrency = v;
        self
    }
    pub fn duration_secs(mut self, v: u64) -> Self {
        self.duration_secs = v;
        self
    }
    pub fn timeout_secs(mut self, v: u64) -> Self {
        self.timeout_secs = v;
        self
    }
    pub fn chaos(mut self, v: bool) -> Self {
        self.chaos = v;
        self
    }
    pub fn chaos_rate(mut self, v: f32) -> Self {
        self.chaos_rate = v;
        self
    }
    pub fn no_progress(mut self, v: bool) -> Self {
        self.no_progress = v;
        self
    }
    pub fn method(mut self, v: &str) -> Self {
        self.method = v.to_string();
        self
    }
    pub fn body(mut self, v: Option<String>) -> Self {
        self.body = v;
        self
    }
    pub fn form(mut self, v: Option<Vec<(String, String)>>) -> Self {
        self.form = v;
        self
    }
    pub fn headers(mut self, v: Option<Vec<(String, String)>>) -> Self {
        self.headers = v;
        self
    }
    pub fn ws_mode(mut self, v: WsMode) -> Self {
        self.ws_mode = v;
        self
    }
    pub fn ws_payload(mut self, v: Option<String>) -> Self {
        self.ws_payload = v;
        self
    }
    pub fn ws_persistent(mut self, v: bool) -> Self {
        self.ws_persistent = v;
        self
    }
    pub fn ws_keepalive_secs(mut self, v: Option<u64>) -> Self {
        self.ws_keepalive_secs = v;
        self
    }
    pub fn ws_max_messages(mut self, v: Option<u64>) -> Self {
        self.ws_max_messages = v;
        self
    }
    pub fn ws_role(mut self, v: Option<String>) -> Self {
        self.ws_role = v;
        self
    }
    pub fn ws_publish_interval_ms(mut self, v: Option<u64>) -> Self {
        self.ws_publish_interval_ms = v;
        self
    }
    pub fn ws_subscribers(mut self, v: Option<usize>) -> Self {
        self.ws_subscribers = v;
        self
    }
    pub fn grpc_service(mut self, v: Option<String>) -> Self {
        self.grpc_service = v;
        self
    }
    pub fn grpc_method(mut self, v: Option<String>) -> Self {
        self.grpc_method = v;
        self
    }
    pub fn grpc_payload(mut self, v: Option<String>) -> Self {
        self.grpc_payload = v;
        self
    }
    pub fn grpc_deadline_ms(mut self, v: Option<u64>) -> Self {
        self.grpc_deadline_ms = v;
        self
    }
    pub fn proto_path(mut self, v: Option<String>) -> Self {
        self.proto_path = v;
        self
    }
    pub fn grpc_use_reflection(mut self, v: bool) -> Self {
        self.grpc_use_reflection = v;
        self
    }
    pub fn http3_enabled(mut self, v: bool) -> Self {
        self.http3_enabled = v;
        self
    }
    pub fn quic_zero_rtt(mut self, v: bool) -> Self {
        self.quic_zero_rtt = v;
        self
    }
    pub fn quic_max_idle_timeout_ms(mut self, v: Option<u64>) -> Self {
        self.quic_max_idle_timeout_ms = v;
        self
    }
    pub fn sse_enabled(mut self, v: bool) -> Self {
        self.sse_enabled = v;
        self
    }
    pub fn sse_max_events(mut self, v: Option<u64>) -> Self {
        self.sse_max_events = v;
        self
    }
    pub fn output_dir(mut self, v: Option<String>) -> Self {
        self.output_dir = v;
        self
    }
    pub fn no_save(mut self, v: bool) -> Self {
        self.no_save = v;
        self
    }

    pub fn build(self) -> TestConfig {
        TestConfig {
            url: self.url,
            concurrency: self.concurrency,
            duration_secs: self.duration_secs,
            timeout_secs: self.timeout_secs,
            chaos: self.chaos,
            chaos_rate: self.chaos_rate,
            no_progress: self.no_progress,
            method: self.method,
            body: self.body,
            form: self.form,
            headers: self.headers,
            ws_mode: self.ws_mode,
            ws_payload: self.ws_payload,
            grpc_service: self.grpc_service,
            grpc_method: self.grpc_method,
            grpc_payload: self.grpc_payload,
            grpc_deadline_ms: self.grpc_deadline_ms,
            proto_path: self.proto_path,
            grpc_use_reflection: self.grpc_use_reflection,
            ws_persistent: self.ws_persistent,
            ws_keepalive_secs: self.ws_keepalive_secs,
            ws_max_messages: self.ws_max_messages,
            ws_role: self.ws_role,
            ws_publish_interval_ms: self.ws_publish_interval_ms,
            ws_subscribers: self.ws_subscribers,
            http3_enabled: self.http3_enabled,
            quic_zero_rtt: self.quic_zero_rtt,
            quic_max_idle_timeout_ms: self.quic_max_idle_timeout_ms,
            sse_enabled: self.sse_enabled,
            sse_max_events: self.sse_max_events,
            output_dir: self.output_dir,
            no_save: self.no_save,
        }
    }
}

#[derive(Debug, Clone)]
#[pyclass(from_py_object)]
pub enum LoadProfile {
    Constant {
        concurrency: usize,
        duration_secs: u64,
    },
    Ramp {
        start_concurrency: usize,
        target_concurrency: usize,
        ramp_secs: u64,
        hold_secs: u64,
    },
    Spike {
        baseline_concurrency: usize,
        peak_concurrency: usize,
        pre_spike_secs: u64,
        spike_secs: u64,
        post_spike_secs: u64,
    },
}

#[pymethods]
impl LoadProfile {
    #[staticmethod]
    #[pyo3(signature = (concurrency=10, duration_secs=10))]
    fn constant(concurrency: usize, duration_secs: u64) -> Self {
        LoadProfile::Constant {
            concurrency,
            duration_secs,
        }
    }

    #[staticmethod]
    fn ramp(
        start_concurrency: usize,
        target_concurrency: usize,
        ramp_secs: u64,
        hold_secs: u64,
    ) -> Self {
        LoadProfile::Ramp {
            start_concurrency,
            target_concurrency,
            ramp_secs,
            hold_secs,
        }
    }

    #[staticmethod]
    fn spike(
        baseline_concurrency: usize,
        peak_concurrency: usize,
        pre_spike_secs: u64,
        spike_secs: u64,
        post_spike_secs: u64,
    ) -> Self {
        LoadProfile::Spike {
            baseline_concurrency,
            peak_concurrency,
            pre_spike_secs,
            spike_secs,
            post_spike_secs,
        }
    }

    pub fn total_duration(&self) -> u64 {
        match self {
            LoadProfile::Constant { duration_secs, .. } => *duration_secs,
            LoadProfile::Ramp {
                ramp_secs,
                hold_secs,
                ..
            } => ramp_secs + hold_secs,
            LoadProfile::Spike {
                pre_spike_secs,
                spike_secs,
                post_spike_secs,
                ..
            } => pre_spike_secs + spike_secs + post_spike_secs,
        }
    }

    pub fn max_concurrency(&self) -> usize {
        match self {
            LoadProfile::Constant { concurrency, .. } => *concurrency,
            LoadProfile::Ramp {
                target_concurrency, ..
            } => *target_concurrency,
            LoadProfile::Spike {
                peak_concurrency, ..
            } => *peak_concurrency,
        }
    }

    pub fn target_concurrency(&self, elapsed: Duration) -> usize {
        let t = elapsed.as_secs();
        match self {
            LoadProfile::Constant { concurrency, .. } => *concurrency,
            LoadProfile::Ramp {
                start_concurrency,
                target_concurrency,
                ramp_secs,
                hold_secs: _,
            } => {
                if t < *ramp_secs {
                    if *ramp_secs == 0 {
                        return *target_concurrency;
                    }
                    let progress = t as f64 / *ramp_secs as f64;
                    let range = *target_concurrency as f64 - *start_concurrency as f64;
                    (*start_concurrency as f64 + range * progress).round() as usize
                } else {
                    *target_concurrency
                }
            }
            LoadProfile::Spike {
                baseline_concurrency,
                peak_concurrency,
                pre_spike_secs,
                spike_secs,
                ..
            } => {
                if t < *pre_spike_secs {
                    *baseline_concurrency
                } else if t < pre_spike_secs + spike_secs {
                    *peak_concurrency
                } else {
                    *baseline_concurrency
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builder_defaults() {
        let c = TestConfig::builder("http://127.0.0.1:8080".into()).build();
        assert_eq!(c.url, "http://127.0.0.1:8080");
        assert_eq!(c.concurrency, 10);
        assert_eq!(c.duration_secs, 10);
        assert_eq!(c.timeout_secs, 10);
        assert!(!c.chaos);
        assert_eq!(c.chaos_rate, DEFAULT_CHAOS_RATE);
        assert_eq!(c.method, "GET");
        assert!(c.body.is_none());
        assert!(c.headers.is_none());
        assert_eq!(c.ws_mode, WsMode::Handshake);
        assert!(!c.ws_persistent);
        assert!(!c.http3_enabled);
        assert!(!c.no_save);
    }

    #[test]
    fn builder_custom_overrides() {
        let c = TestConfig::builder("grpc://localhost:50051".into())
            .concurrency(50)
            .duration_secs(30)
            .timeout_secs(5)
            .chaos(true)
            .chaos_rate(0.25)
            .method("POST")
            .ws_mode(WsMode::Stream)
            .ws_persistent(true)
            .ws_role(Some("publisher".into()))
            .grpc_service(Some("pkg.Svc".into()))
            .grpc_method(Some("Method".into()))
            .build();
        assert_eq!(c.url, "grpc://localhost:50051");
        assert_eq!(c.concurrency, 50);
        assert_eq!(c.duration_secs, 30);
        assert_eq!(c.timeout_secs, 5);
        assert!(c.chaos);
        assert_eq!(c.chaos_rate, 0.25);
        assert_eq!(c.method, "POST");
        assert_eq!(c.ws_mode, WsMode::Stream);
        assert!(c.ws_persistent);
        assert_eq!(c.ws_role.as_deref(), Some("publisher"));
        assert_eq!(c.grpc_service.as_deref(), Some("pkg.Svc"));
        assert_eq!(c.grpc_method.as_deref(), Some("Method"));
    }

    #[test]
    fn for_protocol_detection_uses_builder() {
        let c = TestConfig::for_protocol_detection("grpc://localhost:50051".into(), 10, 30, 10);
        assert_eq!(c.url, "grpc://localhost:50051");
        assert_eq!(c.concurrency, 10);
        assert_eq!(c.duration_secs, 30);
        assert!(!c.ws_persistent);
        assert!(!c.chaos);
    }

    #[test]
    fn pyo3_new_with_defaults() {
        let c = TestConfig::new(
            "http://127.0.0.1:8080".into(),
            10,
            10,
            10,
            false,
            0.1,
            false,
            "GET",
            None,
            None,
            None,
            WsMode::Handshake,
            None,
            None,
            None,
            None,
            None,
            None,
            false,
            false,
            None,
            None,
            None,
            None,
            None,
            false,
            false,
            None,
            false,
            None,
            None,
            false,
        );
        assert_eq!(c.url, "http://127.0.0.1:8080");
        assert_eq!(c.concurrency, 10);
        assert!(!c.ws_persistent);
    }

    #[test]
    fn pyo3_new_with_custom_values() {
        let headers = vec![("X-Custom".to_string(), "value".to_string())];
        let c = TestConfig::new(
            "http://127.0.0.1:8080".into(),
            50,
            30,
            5,
            true,
            0.25,
            false,
            "POST",
            Some(r#"{"key":"val"}"#.into()),
            None,
            Some(headers),
            WsMode::PingPong,
            None,
            Some("mypackage.MyService".into()),
            Some("MyMethod".into()),
            Some("dGVzdA==".into()),
            Some(5000),
            None,
            false,
            true,
            Some(300),
            None,
            Some("publisher".into()),
            Some(100),
            Some(5),
            false,
            false,
            None,
            false,
            None,
            None,
            false,
        );
        assert_eq!(c.concurrency, 50);
        assert!(c.chaos);
        assert_eq!(c.ws_mode, WsMode::PingPong);
        assert!(c.ws_persistent);
        assert_eq!(c.ws_keepalive_secs, Some(300));
        assert_eq!(c.ws_role.as_deref(), Some("publisher"));
    }

    // LoadProfile tests

    #[test]
    fn constant_total_duration() {
        let p = LoadProfile::Constant {
            concurrency: 10,
            duration_secs: 30,
        };
        assert_eq!(p.total_duration(), 30);
    }

    #[test]
    fn constant_max_concurrency() {
        let p = LoadProfile::Constant {
            concurrency: 50,
            duration_secs: 10,
        };
        assert_eq!(p.max_concurrency(), 50);
    }

    #[test]
    fn constant_target_concurrency() {
        let p = LoadProfile::Constant {
            concurrency: 25,
            duration_secs: 10,
        };
        assert_eq!(p.target_concurrency(Duration::from_secs(0)), 25);
        assert_eq!(p.target_concurrency(Duration::from_secs(5)), 25);
        assert_eq!(p.target_concurrency(Duration::from_secs(10)), 25);
    }

    #[test]
    fn ramp_total_duration() {
        let p = LoadProfile::Ramp {
            start_concurrency: 10,
            target_concurrency: 100,
            ramp_secs: 60,
            hold_secs: 30,
        };
        assert_eq!(p.total_duration(), 90);
    }

    #[test]
    fn ramp_max_concurrency() {
        let p = LoadProfile::Ramp {
            start_concurrency: 10,
            target_concurrency: 200,
            ramp_secs: 60,
            hold_secs: 30,
        };
        assert_eq!(p.max_concurrency(), 200);
    }

    #[test]
    fn ramp_target_concurrency_interpolation() {
        let p = LoadProfile::Ramp {
            start_concurrency: 10,
            target_concurrency: 100,
            ramp_secs: 90,
            hold_secs: 10,
        };
        assert_eq!(p.target_concurrency(Duration::from_secs(0)), 10);
        assert_eq!(p.target_concurrency(Duration::from_secs(45)), 55);
        assert_eq!(p.target_concurrency(Duration::from_secs(90)), 100);
        assert_eq!(p.target_concurrency(Duration::from_secs(100)), 100);
    }

    #[test]
    fn ramp_zero_duration() {
        let p = LoadProfile::Ramp {
            start_concurrency: 10,
            target_concurrency: 100,
            ramp_secs: 0,
            hold_secs: 10,
        };
        assert_eq!(p.target_concurrency(Duration::from_secs(0)), 100);
    }

    #[test]
    fn spike_total_duration() {
        let p = LoadProfile::Spike {
            baseline_concurrency: 5,
            peak_concurrency: 500,
            pre_spike_secs: 10,
            spike_secs: 20,
            post_spike_secs: 10,
        };
        assert_eq!(p.total_duration(), 40);
    }

    #[test]
    fn spike_max_concurrency() {
        let p = LoadProfile::Spike {
            baseline_concurrency: 5,
            peak_concurrency: 1000,
            pre_spike_secs: 5,
            spike_secs: 10,
            post_spike_secs: 5,
        };
        assert_eq!(p.max_concurrency(), 1000);
    }

    #[test]
    fn spike_target_concurrency_phases() {
        let p = LoadProfile::Spike {
            baseline_concurrency: 5,
            peak_concurrency: 500,
            pre_spike_secs: 10,
            spike_secs: 20,
            post_spike_secs: 10,
        };
        assert_eq!(p.target_concurrency(Duration::from_secs(0)), 5);
        assert_eq!(p.target_concurrency(Duration::from_secs(9)), 5);
        assert_eq!(p.target_concurrency(Duration::from_secs(10)), 500);
        assert_eq!(p.target_concurrency(Duration::from_secs(29)), 500);
        assert_eq!(p.target_concurrency(Duration::from_secs(30)), 5);
        assert_eq!(p.target_concurrency(Duration::from_secs(39)), 5);
    }
}
