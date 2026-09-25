//! Micro-benchmarks isolating the per-frame overhead of the WebSocket deep-metrics
//! instrumentation (heartbeat tallying + `CountingSink` byte accounting).
//!
//! The `ws_metrics` harness compares a bare pass-through sink against the
//! `CountingSink` wrapper around identical enqueue calls; the delta is the added
//! CPU cost per outbound frame. Run with `cargo bench`.

use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::Bytes;
use criterion::{BenchmarkId, Criterion, black_box, criterion_group, criterion_main};
use futures_util::Sink;
use tokio_tungstenite::tungstenite::Error as WsError;
use tokio_tungstenite::tungstenite::Message;

use _strobengine::protocols::websocket::CountingSink;

/// A pass-through sink that performs no accounting, used as the baseline.
struct NullSink;

impl Sink<Message> for NullSink {
    type Error = WsError;
    fn poll_ready(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), WsError>> {
        Poll::Ready(Ok(()))
    }
    fn start_send(self: Pin<&mut Self>, _item: Message) -> Result<(), WsError> {
        Ok(())
    }
    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), WsError>> {
        Poll::Ready(Ok(()))
    }
    fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), WsError>> {
        Poll::Ready(Ok(()))
    }
}

fn bench_enqueue_overhead(c: &mut Criterion) {
    let mut group = c.benchmark_group("websocket_write_enqueue");

    for size in [64usize, 1024, 65_536] {
        let bytes = Bytes::from(vec![0u8; size]);

        // Baseline: enqueue straight into a pass-through sink.
        group.bench_function(BenchmarkId::new("passthrough", size), |b| {
            let bytes = bytes.clone();
            b.iter(|| {
                let mut sink = NullSink;
                Pin::new(&mut sink)
                    .start_send(black_box(Message::Binary(bytes.clone())))
                    .unwrap();
            });
        });

        // Instrumented: the same enqueue through CountingSink.
        group.bench_function(BenchmarkId::new("counting_sink", size), |b| {
            let bytes = bytes.clone();
            b.iter(|| {
                let mut sink = CountingSink::new(NullSink, 1_048_576);
                Pin::new(&mut sink)
                    .start_send(black_box(Message::Binary(bytes.clone())))
                    .unwrap();
                black_box(sink.in_flight());
            });
        });
    }

    group.finish();
}

criterion_group!(benches, bench_enqueue_overhead);
criterion_main!(benches);
