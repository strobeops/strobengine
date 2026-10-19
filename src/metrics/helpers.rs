use std::time::{SystemTime, UNIX_EPOCH};

/// Conversion factor from microseconds to milliseconds (1 ms = 1,000 us).
pub const MICROS_PER_MILLI: f64 = 1_000.0;

/// Highest trackable value for HDR histograms (1 hour in microseconds).
pub(crate) const HISTOGRAM_HIGHEST_TRACKABLE: u64 = 3_600_000_000;

/// Significant figures for HDR histogram precision.
pub(crate) const HISTOGRAM_SIGNIFICANT_FIGURES: u8 = 3;

/// Get current wall-clock time in nanoseconds since UNIX epoch.
pub fn wallclock_ns() -> u128 {
    ns_since_epoch(SystemTime::now())
}

/// Convert a `SystemTime` to nanoseconds since UNIX epoch.
///
/// Returns `0` if the clock is set before the epoch (e.g. a container/VM
/// clock reset), logging a warning so the silent fallback is observable.
pub(crate) fn ns_since_epoch(when: SystemTime) -> u128 {
    match when.duration_since(UNIX_EPOCH) {
        Ok(d) => d.as_nanos(),
        Err(e) => {
            tracing::warn!(
                error = %e,
                "system clock is before UNIX epoch; returning 0 wall-clock ns"
            );
            0
        }
    }
}

/// Encode a pub/sub payload: 16-byte big-endian nanosecond timestamp prefix + user payload.
pub fn create_pubsub_payload(user_payload: &[u8]) -> Vec<u8> {
    let now_ns = wallclock_ns();
    let mut payload = Vec::with_capacity(16 + user_payload.len());
    payload.extend_from_slice(&now_ns.to_be_bytes());
    payload.extend_from_slice(user_payload);
    payload
}

/// Decode a pub/sub payload, returning (sent_ns, user_payload).
/// Returns None if the data is shorter than 16 bytes.
#[allow(dead_code)]
pub fn parse_pubsub_payload(data: &[u8]) -> Option<(u128, &[u8])> {
    if data.len() < 16 {
        return None;
    }
    let (ts_bytes, rest) = data.split_at(16);
    let ts_array: [u8; 16] = ts_bytes.try_into().ok()?;
    let sent_ns = u128::from_be_bytes(ts_array);
    Some((sent_ns, rest))
}
