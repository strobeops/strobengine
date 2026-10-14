//! Exponential backoff with optional full jitter.
//!
//! Used by the connection manager to space out reconnect attempts to the
//! master without hammering a down service.

use std::time::Duration;

/// Fixed-schedule exponential backoff.
///
/// The deterministic delay for attempt `n` is `base * factor^n`, capped at
/// `max`. When jitter is enabled, [`Backoff::next_delay`] returns a random
/// value in `[0, delay]` (full jitter).
#[derive(Debug, Clone)]
pub struct Backoff {
    base: Duration,
    max: Duration,
    factor: f64,
    jitter: bool,
    attempt: u32,
}

impl Backoff {
    /// Create a backoff starting at `base` and never exceeding `max`.
    pub fn new(base: Duration, max: Duration) -> Self {
        Self {
            base,
            max,
            factor: 2.0,
            jitter: true,
            attempt: 0,
        }
    }

    /// Override the exponential growth factor (default `2.0`).
    pub fn with_factor(mut self, factor: f64) -> Self {
        self.factor = factor.max(1.0);
        self
    }

    /// Enable or disable full jitter (enabled by default).
    pub fn with_jitter(mut self, jitter: bool) -> Self {
        self.jitter = jitter;
        self
    }

    /// Number of delays produced so far.
    pub fn attempt(&self) -> u32 {
        self.attempt
    }

    /// Reset the schedule back to the first attempt.
    pub fn reset(&mut self) {
        self.attempt = 0;
    }

    /// The deterministic (jitter-free) delay for a given attempt index.
    pub fn delay_for(&self, attempt: u32) -> Duration {
        let base = self.base.as_secs_f64();
        let max = self.max.as_secs_f64();
        let scaled = base * self.factor.powi(attempt.min(32) as i32);
        let secs = scaled.min(max);
        Duration::from_secs_f64(secs)
    }

    /// Produce the next delay and advance the attempt counter.
    pub fn next_delay(&mut self) -> Duration {
        let delay = self.delay_for(self.attempt);
        self.attempt = self.attempt.saturating_add(1);
        if self.jitter {
            let factor = fastrand::f64();
            Duration::from_secs_f64(delay.as_secs_f64() * factor)
        } else {
            delay
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deterministic_schedule_grows_and_caps() {
        let backoff = Backoff::new(Duration::from_millis(100), Duration::from_millis(1000))
            .with_jitter(false);
        assert_eq!(backoff.delay_for(0), Duration::from_millis(100));
        assert_eq!(backoff.delay_for(1), Duration::from_millis(200));
        assert_eq!(backoff.delay_for(2), Duration::from_millis(400));
        assert_eq!(backoff.delay_for(3), Duration::from_millis(800));
        // Capped at max.
        assert_eq!(backoff.delay_for(4), Duration::from_millis(1000));
        assert_eq!(backoff.delay_for(50), Duration::from_millis(1000));
    }

    #[test]
    fn next_delay_advances_attempt() {
        let mut backoff =
            Backoff::new(Duration::from_millis(10), Duration::from_millis(80)).with_jitter(false);
        assert_eq!(backoff.attempt(), 0);
        assert_eq!(backoff.next_delay(), Duration::from_millis(10));
        assert_eq!(backoff.next_delay(), Duration::from_millis(20));
        assert_eq!(backoff.attempt(), 2);

        backoff.reset();
        assert_eq!(backoff.attempt(), 0);
        assert_eq!(backoff.next_delay(), Duration::from_millis(10));
    }

    #[test]
    fn jitter_stays_within_bounds() {
        // factor 1.0 keeps the un-jittered delay constant at 200ms.
        let mut backoff = Backoff::new(Duration::from_millis(200), Duration::from_secs(10))
            .with_factor(1.0)
            .with_jitter(true);
        for _ in 0..100 {
            let delay = backoff.next_delay();
            assert!(delay <= Duration::from_millis(200));
        }
    }
}
