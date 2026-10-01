//! Micro-benchmarks isolating the per-request overhead added by the gRPC
//! deep-metrics instrumentation (`build_frame` request framing + the in-flight
//! `ActiveGuard` atomic that backs the stream-concurrency gauge).
//!
//! The atomic pattern mirrors `grpc_h2::ActiveGuard` (fetch_add on send,
//! fetch_sub on completion) which is otherwise private. Run with `cargo bench`.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use criterion::{BenchmarkId, Criterion, black_box, criterion_group, criterion_main};

use _strobengine::protocols::grpc_h2::build_frame;

fn bench_frame(c: &mut Criterion) {
    let mut group = c.benchmark_group("grpc_build_frame");
    for size in [8usize, 256, 8192] {
        let payload = vec![0u8; size];
        group.bench_function(BenchmarkId::new("payload", size), |b| {
            b.iter(|| black_box(build_frame(black_box(&payload))))
        });
    }
    group.finish();
}

fn bench_active_guard(c: &mut Criterion) {
    // Models ActiveGuard: one fetch_add (send) + one load (snapshot) + one
    // fetch_sub (completion) per RPC on the shared connection.
    let mut group = c.benchmark_group("grpc_active_stream_tracking");
    group.bench_function("inc_load_dec", |b| {
        let counter = Arc::new(AtomicU64::new(0));
        b.iter(|| {
            counter.fetch_add(1, Ordering::Relaxed);
            let active = counter.load(Ordering::Relaxed);
            counter.fetch_sub(1, Ordering::Relaxed);
            black_box(active);
        })
    });
    group.finish();
}

fn bench_payload_clone(c: &mut Criterion) {
    // Per-iteration payload copy: Vec<u8>::clone() (alloc + memcpy) vs the
    // Bytes::clone() we now use (O(1) Arc refcount bump) in the gRPC engines.
    let mut group = c.benchmark_group("grpc_payload_clone");
    for size in [16usize, 1024, 65_536] {
        let v = vec![0u8; size];
        let b = Bytes::from(v.clone());
        group.bench_function(BenchmarkId::new("vec_clone", size), |benches| {
            benches.iter(|| {
                let clone = black_box(&v).clone();
                black_box(&clone[..]);
            })
        });
        group.bench_function(BenchmarkId::new("bytes_clone", size), |benches| {
            benches.iter(|| {
                let clone = black_box(&b).clone();
                black_box(&clone[..]);
            })
        });
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_frame,
    bench_active_guard,
    bench_payload_clone
);
criterion_main!(benches);
