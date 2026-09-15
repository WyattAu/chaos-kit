#![allow(clippy::unwrap_used, clippy::expect_used)]
//! Tower integration: every fault applied through `ChaosLayer`, on a real
//! (current-thread) tokio runtime with a frozen clock.

use chaos_kit::{
    chaos_layer, ChaosError, ChaosSchedule, Direction, Fault, FaultKind, PausableClock,
};
use std::future::ready;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;
use tower::{Layer, Service};

/// Minimal inner service: counts calls, always answers "ok".
#[derive(Clone)]
struct Echo {
    calls: Arc<AtomicUsize>,
}

impl Echo {
    fn new() -> Self {
        Self {
            calls: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn calls(&self) -> usize {
        self.calls.load(Ordering::Relaxed)
    }
}

impl Service<()> for Echo {
    type Response = &'static str;
    type Error = ChaosError;
    type Future = std::future::Ready<Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, (): ()) -> Self::Future {
        self.calls.fetch_add(1, Ordering::Relaxed);
        ready(Ok("ok"))
    }
}

#[tokio::test]
async fn latency_fault_delays_forwarding_on_virtual_time() {
    let clock = PausableClock::pause();
    let schedule = ChaosSchedule::seeded(7).fault_at(0, Fault::Latency(Duration::from_secs(1)));
    let probe = schedule.clone();
    let echo = Echo::new();

    let wall = std::time::Instant::now();
    let mut layer = chaos_layer(schedule).layer(echo.clone());
    let handle = tokio::spawn(async move {
        let start = tokio::time::Instant::now();
        let response = Service::call(&mut layer, ()).await;
        (start.elapsed(), response)
    });

    // The spawned task is parked on a 1s virtual sleep: 999ms is not enough.
    tokio::task::yield_now().await;
    clock.advance(Duration::from_millis(999)).await;
    assert!(!handle.is_finished(), "latency fault resolved 1ms early");

    // Step past the deadline (timer wheels may release a tick late: slack).
    clock.advance(Duration::from_millis(101)).await;
    let (elapsed, response) = handle.await.expect("join");
    assert_eq!(response.expect("forwarded"), "ok");

    // At least one virtual second of latency, at near-zero wall cost.
    assert!(elapsed >= Duration::from_secs(1), "elapsed {elapsed:?}");
    assert!(
        elapsed <= Duration::from_secs(1) + Duration::from_millis(200),
        "elapsed {elapsed:?}"
    );
    assert!(
        wall.elapsed() < Duration::from_secs(1),
        "the 1s latency cost real wall time"
    );
    assert_eq!(echo.calls(), 1);
    assert_eq!(probe.recorder().injected(FaultKind::Latency), 1);
}

#[tokio::test]
async fn error_fault_returns_the_scripted_chaos_error() {
    let schedule = ChaosSchedule::seeded(7)
        .fault_at(0, Fault::Error(ChaosError::Timeout))
        .fault_at(1, Fault::Error(ChaosError::Custom("scripted".into())));
    let probe = schedule.clone();
    let echo = Echo::new();

    let mut layer = chaos_layer(schedule).layer(echo.clone());

    let err = Service::call(&mut layer, ()).await.expect_err("timeout");
    assert_eq!(err, ChaosError::Timeout);
    let err = Service::call(&mut layer, ()).await.expect_err("custom");
    assert_eq!(err, ChaosError::Custom("scripted".into()));

    assert_eq!(echo.calls(), 0, "inner service must never see the call");
    assert_eq!(probe.recorder().injected(FaultKind::Error), 2);
}

#[tokio::test]
async fn partition_faults_sever_before_the_inner_service() {
    let schedule = ChaosSchedule::seeded(7)
        .fault_at(
            1,
            Fault::Partition {
                direction: Direction::Egress,
                for_duration: Duration::from_secs(30),
            },
        )
        .fault_at(
            3,
            Fault::Partition {
                direction: Direction::Ingress,
                for_duration: Duration::from_secs(30),
            },
        );
    let probe = schedule.clone();
    let echo = Echo::new();

    let mut layer = chaos_layer(schedule).layer(echo.clone());

    assert_eq!(Service::call(&mut layer, ()).await.unwrap(), "ok");
    let err = Service::call(&mut layer, ()).await.expect_err("egress cut");
    assert_eq!(err, ChaosError::Unavailable);
    assert_eq!(Service::call(&mut layer, ()).await.unwrap(), "ok");
    let err = Service::call(&mut layer, ())
        .await
        .expect_err("ingress cut");
    assert_eq!(err, ChaosError::Unavailable);

    assert_eq!(
        echo.calls(),
        2,
        "partitioned calls never reach the inner service"
    );
    assert_eq!(probe.recorder().injected(FaultKind::Partition), 2);
}

#[tokio::test]
async fn throttle_drop_ratio_holds_within_bounds_over_1000_seeded_calls() {
    let schedule = ChaosSchedule::seeded(0xFEED).fault_every(1, Fault::Throttle { rate: 0.3 });
    let probe = schedule.clone();
    let echo = Echo::new();

    let mut layer = chaos_layer(schedule).layer(echo.clone());

    let mut dropped = 0_usize;
    for index in 0_u64..1000 {
        let result = Service::call(&mut layer, ()).await;
        // The layer's decision must be exactly the schedule's seeded draw.
        assert_eq!(result.is_err(), probe.drops(index, 0.3), "index {index}");
        if result.is_err() {
            dropped += 1;
        }
    }

    // rate 0.3 over 1000 seeded draws: 0.3 ± 0.1 (the seed fixes the exact count).
    #[allow(clippy::cast_precision_loss)] // ratio, not identity
    let ratio = dropped as f64 / 1000.0;
    assert!(
        (0.2..=0.4).contains(&ratio),
        "drop ratio {ratio} outside 0.2..=0.4"
    );
    assert_eq!(dropped, probe.recorder().injected(FaultKind::Throttle));
    assert_eq!(echo.calls(), 1000 - dropped);
}

#[tokio::test]
async fn cpu_fault_burns_then_forwards() {
    let schedule = ChaosSchedule::seeded(7).fault_at(0, Fault::Cpu(Duration::from_nanos(1)));
    let probe = schedule.clone();
    let echo = Echo::new();

    let mut layer = chaos_layer(schedule).layer(echo.clone());
    let response = Service::call(&mut layer, ()).await;

    assert_eq!(response.unwrap(), "ok");
    assert_eq!(echo.calls(), 1);
    assert_eq!(probe.recorder().injected(FaultKind::Cpu), 1);
}

#[tokio::test]
async fn empty_schedule_is_pure_pass_through() {
    let schedule = ChaosSchedule::seeded(0xABCD);
    let echo = Echo::new();

    let mut layer = chaos_layer(schedule).layer(echo.clone());
    for _ in 0..100 {
        assert_eq!(Service::call(&mut layer, ()).await.unwrap(), "ok");
    }

    assert_eq!(echo.calls(), 100);
    assert_eq!(layer.recorder().total(), 0);
    assert_eq!(layer.schedule().seed(), 0xABCD);
}

#[tokio::test]
async fn mixed_schedule_counts_every_kind_exactly() {
    // Registration order (later rules override earlier ones on overlap):
    //   every 4      -> Latency      (0, 4, 8, 12, 16, 20, 24, 28)
    //   at 6         -> Error
    //   at 8         -> Partition    (overrides latency at 8)
    //   every 16     -> Throttle 1.0 (0, 16 — overrides latency at 0)
    //   at 15        -> Cpu
    let schedule = ChaosSchedule::seeded(3)
        .fault_every(4, Fault::Latency(Duration::from_millis(5)))
        .fault_at(6, Fault::Error(ChaosError::Unavailable))
        .fault_at(
            8,
            Fault::Partition {
                direction: Direction::Ingress,
                for_duration: Duration::from_secs(1),
            },
        )
        .fault_every(16, Fault::Throttle { rate: 1.0 })
        .fault_at(15, Fault::Cpu(Duration::from_nanos(1)));
    let probe = schedule.clone();
    let echo = Echo::new();

    let mut layer = chaos_layer(schedule).layer(echo.clone());
    for _ in 0..32 {
        let _ = Service::call(&mut layer, ()).await;
    }

    // Latency survives at 4, 12, 20, 24, 28 (0 -> throttle, 8 -> partition).
    assert_eq!(probe.recorder().injected(FaultKind::Latency), 5);
    assert_eq!(probe.recorder().injected(FaultKind::Error), 1);
    assert_eq!(probe.recorder().injected(FaultKind::Partition), 1);
    assert_eq!(probe.recorder().injected(FaultKind::Throttle), 2);
    assert_eq!(probe.recorder().injected(FaultKind::Cpu), 1);
    // Latency and Cpu forward; Error, Partition, and Throttle do not.
    assert_eq!(echo.calls(), 32 - 4);
}

#[test]
fn chaos_error_display_is_stable() {
    assert_eq!(
        ChaosError::Unavailable.to_string(),
        "chaos: dependency unavailable"
    );
    assert_eq!(
        ChaosError::Timeout.to_string(),
        "chaos: dependency timed out"
    );
    assert_eq!(
        ChaosError::Custom("partition".into()).to_string(),
        "chaos: partition"
    );
}
