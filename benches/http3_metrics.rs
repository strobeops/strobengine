//! Micro-benchmark for the per-sample HTTP/3 cwnd aggregation performed by
//! `finalize_metrics`.
//!
//! The `metrics` module is crate-private, so this self-contained accumulator
//! mirrors the exact O(1) min/max/mean update (and the significance gate) that
//! the real `finalize_metrics` http3 branch runs per received sample — measuring
//! the added per-iteration cost of the metric on the hot path. Run with
//! `cargo bench`.

use criterion::{Criterion, black_box, criterion_group, criterion_main};

/// Mirrors the running-counter state finalize_metrics maintains for cwnd.
struct CwndAcc {
    max: u64,
    min: u64,
    sum: u64,
    count: u64,
}

impl CwndAcc {
    fn new() -> Self {
        Self {
            max: 0,
            min: u64::MAX,
            sum: 0,
            count: 0,
        }
    }

    #[inline]
    fn add(&mut self, cwnd: u64, migrations_attempted: u64) {
        // significance gate: cwnd > 0 || migrations_attempted > 0
        if cwnd > 0 || migrations_attempted > 0 {
            if cwnd > self.max {
                self.max = cwnd;
            }
            if cwnd < self.min {
                self.min = cwnd;
            }
            self.sum = self.sum.saturating_add(cwnd);
            self.count += 1;
        }
    }
}

fn bench_cwnd_aggregate(c: &mut Criterion) {
    c.bench_function("http3_cwnd_aggregate", |b| {
        let samples = [2000u64, 1000, 8000, 4000, 0, 3000, 500];
        let mut acc = CwndAcc::new();
        let mut i = 0usize;
        b.iter(|| {
            let cwnd = samples[i % samples.len()];
            i += 1;
            // migrations_attempted is 0 in steady state; still exercise the gate.
            acc.add(black_box(cwnd), black_box(0));
            black_box((acc.max, acc.min, acc.sum, acc.count));
        })
    });
}

criterion_group!(benches, bench_cwnd_aggregate);
criterion_main!(benches);
