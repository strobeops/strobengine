use std::collections::HashMap;

pub const DEFAULT_CHAOS_RATE: f32 = 0.1;

#[derive(Debug, Clone, Copy)]
pub enum ChaosFault {
    LatencySpike { duration_ms: u64 },
    CorruptedPayload,
    MetadataCorruption,
    ConnectionDrop,
}

impl ChaosFault {
    pub fn name(&self) -> &'static str {
        match self {
            ChaosFault::LatencySpike { .. } => "LatencySpike",
            ChaosFault::CorruptedPayload => "CorruptedPayload",
            ChaosFault::MetadataCorruption => "MetadataCorruption",
            ChaosFault::ConnectionDrop => "ConnectionDrop",
        }
    }
}

#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct ChaosMetrics {
    pub total_injected: u64,
    pub faults_by_type: HashMap<String, u64>,
}

impl ChaosMetrics {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record_fault(&mut self, fault: &ChaosFault) {
        self.total_injected += 1;
        *self
            .faults_by_type
            .entry(fault.name().to_string())
            .or_insert(0) += 1;
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct ChaosEngine {
    pub enabled: bool,
    pub rate: f32,
}

impl ChaosEngine {
    pub fn new(enabled: bool, rate: f32) -> Self {
        Self { enabled, rate }
    }

    #[inline]
    pub fn select_fault(&self) -> Option<ChaosFault> {
        if !self.enabled || fastrand::f32() >= self.rate {
            return None;
        }
        match fastrand::u8(0..4) {
            0 => Some(ChaosFault::LatencySpike { duration_ms: 150 }),
            1 => Some(ChaosFault::CorruptedPayload),
            2 => Some(ChaosFault::MetadataCorruption),
            _ => Some(ChaosFault::ConnectionDrop),
        }
    }
}
