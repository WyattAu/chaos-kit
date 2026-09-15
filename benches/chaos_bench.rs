//! Criterion benches: schedule lookup throughput and tower-layer overhead.
//! Targets: schedule lookup in the 1M+/s class, empty-schedule layer
//! overhead in the ns class.
#![allow(clippy::unwrap_used, clippy::expect_used, missing_docs)]

use chaos_kit::{chaos_layer, ChaosError, ChaosSchedule, Fault};
use criterion::{criterion_group, criterion_main, Criterion};
use std::hint::black_box;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;
use tower::{Layer, Service};

/// An inner service that costs nothing, so bench numbers isolate chaos.
struct Noop;

impl Service<()> for Noop {
    type Response = ();
    type Error = ChaosError;
    type Future = std::future::Ready<Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, (): ()) -> Self::Future {
        std::future::ready(Ok(()))
    }
}

fn populated_schedule() -> ChaosSchedule {
    ChaosSchedule::seeded(0xC0_FFEE)
        .fault_every(2, Fault::Latency(Duration::from_millis(1)))
        .fault_at(7, Fault::Error(ChaosError::Timeout))
        .window(100, 200, Fault::Throttle { rate: 0.3 })
}

fn bench_schedule_fault_for_lookup(c: &mut Criterion) {
    let schedule = populated_schedule();
    let index = AtomicU64::new(0);
    c.bench_function("schedule_fault_for_lookup", |b| {
        b.iter(|| {
            let index = index.fetch_add(1, Ordering::Relaxed);
            black_box(schedule.fault_for(black_box(index)))
        });
    });
}

fn bench_schedule_draw(c: &mut Criterion) {
    let schedule = ChaosSchedule::seeded(0xC0_FFEE);
    let index = AtomicU64::new(0);
    c.bench_function("schedule_draw", |b| {
        b.iter(|| {
            let index = index.fetch_add(1, Ordering::Relaxed);
            black_box(schedule.draw(black_box(index)))
        });
    });
}

fn bench_layer_overhead_empty_schedule(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("bench runtime");
    let mut service = chaos_layer(ChaosSchedule::seeded(1)).layer(Noop);
    c.bench_function("layer_overhead_empty_schedule", |b| {
        b.iter(|| {
            // The future (and its tokio timer, if a latency fault fires)
            // must be created inside the runtime context.
            black_box(rt.block_on(async { tower::Service::call(&mut service, ()).await.is_ok() }));
        });
    });
}

fn bench_layer_overhead_inert_rules(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("bench runtime");
    // Seeded but inert rules (a saturating throttle never drops): the full
    // lookup + draw runs every call, with no timers or allocations.
    let schedule = populated_schedule().fault_every(1, Fault::Throttle { rate: 0.0 });
    let mut service = chaos_layer(schedule).layer(Noop);
    c.bench_function("layer_overhead_inert_rules", |b| {
        b.iter(|| {
            // The future (and its tokio timer, if a latency fault fires)
            // must be created inside the runtime context.
            black_box(rt.block_on(async { tower::Service::call(&mut service, ()).await.is_ok() }));
        });
    });
}

fn bench_recorder_throughput(c: &mut Criterion) {
    let schedule = Arc::new(ChaosSchedule::seeded(1));
    let recorder = Arc::clone(&schedule);
    c.bench_function("recorder_record", |b| {
        b.iter(|| {
            black_box(recorder.recorder()).record(black_box(chaos_kit::FaultKind::Latency));
        });
    });
}

criterion_group!(
    benches,
    bench_schedule_fault_for_lookup,
    bench_schedule_draw,
    bench_layer_overhead_empty_schedule,
    bench_layer_overhead_inert_rules,
    bench_recorder_throughput
);
criterion_main!(benches);
