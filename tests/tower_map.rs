#![allow(clippy::unwrap_used, clippy::expect_used)]
//! Tower integration for [`chaos_layer_map`]: every fault applied through
//! the error-mapping bridge, on a real (current-thread) tokio runtime with
//! a frozen clock. Mirrors `tower_faults.rs`, with the closure deciding
//! what each injected fault becomes.

use chaos_kit::{
    chaos_layer, chaos_layer_map, ChaosError, ChaosSchedule, Direction, Fault, FaultKind,
    PausableClock,
};
use std::future::{ready, Future};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll, Waker};
use std::time::Duration;
use tower::{Layer, Service};

/// Minimal inner service: counts calls, always answers "ok".
#[derive(Clone, Debug)]
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

/// A host service whose error type is its own — no `From<ChaosError>`.
#[derive(Clone, Debug)]
struct FallibleEcho {
    calls: Arc<AtomicUsize>,
}

impl FallibleEcho {
    fn new() -> Self {
        Self {
            calls: Arc::new(AtomicUsize::new(0)),
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
struct HostError {
    chaos: ChaosError,
}

impl Service<()> for FallibleEcho {
    type Response = &'static str;
    type Error = HostError;
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
async fn map_error_fault_becomes_a_response() {
    let schedule = ChaosSchedule::seeded(7)
        .fault_at(0, Fault::Error(ChaosError::Timeout))
        .fault_at(1, Fault::Error(ChaosError::Custom("scripted".into())));
    let probe = schedule.clone();
    let echo = Echo::new();

    let mut layer = chaos_layer_map(schedule, |error: ChaosError| {
        Ok::<_, ChaosError>(if matches!(error, ChaosError::Timeout) {
            "faulted: timeout"
        } else {
            "faulted: scripted"
        })
    })
    .layer(echo.clone());

    // Both scripted errors surface as genuine responses — the closure
    // folded them in — and the inner service is never reached.
    assert_eq!(
        Service::call(&mut layer, ()).await.unwrap(),
        "faulted: timeout"
    );
    assert_eq!(
        Service::call(&mut layer, ()).await.unwrap(),
        "faulted: scripted"
    );

    assert_eq!(echo.calls(), 0);
    assert_eq!(probe.recorder().injected(FaultKind::Error), 2);
}

#[tokio::test]
async fn map_partition_fault_calls_the_closure_with_unavailable() {
    let schedule = ChaosSchedule::seeded(7).fault_at(
        1,
        Fault::Partition {
            direction: Direction::Egress,
            for_duration: Duration::from_secs(30),
        },
    );
    let probe = schedule.clone();
    let echo = Echo::new();

    let mut layer = chaos_layer_map(schedule, |error: ChaosError| {
        Ok::<_, ChaosError>(if matches!(error, ChaosError::Unavailable) {
            "partitioned"
        } else {
            "unexpected"
        })
    })
    .layer(echo.clone());

    assert_eq!(Service::call(&mut layer, ()).await.unwrap(), "ok");
    assert_eq!(Service::call(&mut layer, ()).await.unwrap(), "partitioned");
    assert_eq!(echo.calls(), 1);
    assert_eq!(probe.recorder().injected(FaultKind::Partition), 1);
}

#[tokio::test]
async fn map_latency_fault_delays_forwarding_on_virtual_time() {
    let clock = PausableClock::pause();
    let schedule = ChaosSchedule::seeded(7).fault_at(0, Fault::Latency(Duration::from_secs(1)));
    let probe = schedule.clone();
    let echo = Echo::new();

    let mut layer = chaos_layer_map(schedule, |_error: ChaosError| {
        Ok::<_, ChaosError>("faulted")
    })
    .layer(echo.clone());
    let handle = tokio::spawn(async move { Service::call(&mut layer, ()).await });

    tokio::task::yield_now().await;
    clock.advance(Duration::from_millis(999)).await;
    assert!(!handle.is_finished(), "latency fault resolved 1ms early");
    clock.advance(Duration::from_millis(101)).await;

    assert_eq!(handle.await.expect("join").unwrap(), "ok");
    assert_eq!(echo.calls(), 1);
    assert_eq!(probe.recorder().injected(FaultKind::Latency), 1);
}

#[tokio::test]
async fn map_throttle_drops_are_seeded_and_exact() {
    let schedule = ChaosSchedule::seeded(0xFEED).fault_every(1, Fault::Throttle { rate: 0.3 });
    let probe = schedule.clone();
    let echo = Echo::new();

    let mut layer = chaos_layer_map(schedule, |_error: ChaosError| {
        Ok::<_, ChaosError>("throttled")
    })
    .layer(echo.clone());

    for index in 0_u64..200 {
        let result = Service::call(&mut layer, ()).await.unwrap();
        // The mapped outcome must be exactly the schedule's seeded draw.
        assert_eq!(
            result == "throttled",
            probe.drops(index, 0.3),
            "index {index}"
        );
    }
}

#[tokio::test]
async fn map_cpu_fault_burns_then_forwards() {
    // A zero-duration burn at index 1 takes the early-return path.
    let schedule = ChaosSchedule::seeded(7)
        .fault_at(0, Fault::Cpu(Duration::from_nanos(1)))
        .fault_at(1, Fault::Cpu(Duration::ZERO));
    let probe = schedule.clone();
    let echo = Echo::new();

    let mut layer = chaos_layer_map(schedule, |_error: ChaosError| {
        Ok::<_, ChaosError>("faulted")
    })
    .layer(echo.clone());

    assert_eq!(Service::call(&mut layer, ()).await.unwrap(), "ok");
    assert_eq!(Service::call(&mut layer, ()).await.unwrap(), "ok");
    assert_eq!(echo.calls(), 2);
    assert_eq!(probe.recorder().injected(FaultKind::Cpu), 2);
}

#[tokio::test]
async fn map_err_side_propagates_the_host_error() {
    // The other half of the bridge: a service whose error type is its own
    // (no From<ChaosError>) receives the mapped error.
    let schedule = ChaosSchedule::seeded(7).fault_at(0, Fault::Error(ChaosError::Timeout));
    let echo = FallibleEcho::new();
    let calls = echo.clone();

    let mut layer = chaos_layer_map(schedule, |error: ChaosError| {
        Err::<&'static str, _>(HostError { chaos: error })
    })
    .layer(echo);

    let err = Service::call(&mut layer, ()).await.expect_err("mapped");
    assert_eq!(
        err,
        HostError {
            chaos: ChaosError::Timeout
        }
    );
    assert_eq!(calls.calls(), 0);
}

impl FallibleEcho {
    fn calls(&self) -> usize {
        self.calls.load(Ordering::Relaxed)
    }
}

#[tokio::test]
async fn map_clean_calls_forward_untouched_through_fallible_inner() {
    let schedule = ChaosSchedule::seeded(7).fault_at(1, Fault::Error(ChaosError::Timeout));
    let echo = FallibleEcho::new();
    let calls = echo.clone();

    let mut layer = chaos_layer_map(schedule, |error: ChaosError| {
        Err::<&'static str, _>(HostError { chaos: error })
    })
    .layer(echo);

    assert_eq!(Service::call(&mut layer, ()).await.unwrap(), "ok");
    assert_eq!(
        Service::call(&mut layer, ()).await.unwrap_err().chaos,
        ChaosError::Timeout
    );
    assert_eq!(calls.calls(), 1);
}

#[tokio::test]
async fn cloned_map_layers_share_the_call_counter() {
    let schedule = ChaosSchedule::seeded(7).fault_at(0, Fault::Error(ChaosError::Timeout));
    let make = chaos_layer_map(schedule, |error: ChaosError| {
        Ok::<_, ChaosError>(if matches!(error, ChaosError::Timeout) {
            "timeout"
        } else {
            "faulted"
        })
    });

    let mut a = make.layer(Echo::new());
    let mut b = a.clone();

    // Index 0 faults through the first clone...
    assert_eq!(Service::call(&mut a, ()).await.unwrap(), "timeout");
    // ...index 1 is clean through the second (shared counter).
    assert_eq!(Service::call(&mut b, ()).await.unwrap(), "ok");
}

#[tokio::test]
async fn poll_ready_accessors_and_debug() {
    let schedule = ChaosSchedule::seeded(0xABCD);
    let make = chaos_layer_map(schedule, |_error: ChaosError| {
        Ok::<_, ChaosError>("faulted")
    });

    let debug_make = format!("{make:?}");
    assert!(debug_make.contains("ChaosMakeMapLayer"), "{debug_make}");

    let mut layer = make.layer(Echo::new());
    let waker = Waker::noop();
    let mut cx = Context::from_waker(waker);
    assert!(matches!(layer.poll_ready(&mut cx), Poll::Ready(Ok(()))));
    assert_eq!(layer.schedule().seed(), 0xABCD);
    assert_eq!(layer.recorder().total(), 0);

    let debug_layer = format!("{layer:?}");
    assert!(debug_layer.contains("ChaosMapLayer"), "{debug_layer}");
}

#[tokio::test]
async fn faulted_futures_stay_pending_after_readiness() {
    // A second poll after readiness is a contract violation; both futures
    // must stay pending rather than panic (or replay the fault).
    let waker = Waker::noop();
    let mut cx = Context::from_waker(waker);

    let mut plain =
        chaos_layer(ChaosSchedule::seeded(7).fault_at(0, Fault::Error(ChaosError::Timeout)))
            .layer(Echo::new());
    let mut mapped = chaos_layer_map(
        ChaosSchedule::seeded(7).fault_at(0, Fault::Error(ChaosError::Timeout)),
        |error: ChaosError| Err::<&'static str, _>(ChaosError::Custom(error.to_string().into())),
    )
    .layer(Echo::new());

    let mut plain_fut = std::pin::pin!(Service::call(&mut plain, ()));
    assert!(plain_fut.as_mut().poll(&mut cx).is_ready());
    assert_eq!(plain_fut.as_mut().poll(&mut cx), Poll::Pending);

    let mut mapped_fut = std::pin::pin!(Service::call(&mut mapped, ()));
    assert!(mapped_fut.as_mut().poll(&mut cx).is_ready());
    assert_eq!(mapped_fut.as_mut().poll(&mut cx), Poll::Pending);
}
