#![allow(clippy::unwrap_used, clippy::expect_used)]
//! `PausableClock`: loud failures outside a test runtime, precise control
//! inside one.

use chaos_kit::PausableClock;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

#[test]
fn pause_outside_any_runtime_panics_loudly() {
    // A plain thread has no tokio runtime: the clock must refuse to arm.
    let result = std::thread::spawn(|| {
        let _ = PausableClock::pause();
    })
    .join();

    assert!(result.is_err(), "pause must panic outside a tokio runtime");
}

#[tokio::test(flavor = "multi_thread")]
async fn pause_on_a_multi_thread_runtime_panics_loudly() {
    // Tokio cannot freeze time on the multi-thread scheduler: the clock
    // must refuse with the "use #[tokio::test]" message.
    let result = std::panic::catch_unwind(|| {
        let _ = PausableClock::pause();
    });

    assert!(
        result.is_err(),
        "pause must panic on a multi-thread runtime"
    );
}

#[tokio::test]
async fn frozen_time_only_moves_when_advanced() {
    let clock = PausableClock::pause();
    let fired = Arc::new(AtomicBool::new(false));

    let flag = Arc::clone(&fired);
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(1)).await;
        flag.store(true, Ordering::Relaxed);
    });

    tokio::task::yield_now().await;
    clock.advance(Duration::from_millis(999)).await;
    assert!(
        !fired.load(Ordering::Relaxed),
        "timer fired 1ms before its deadline"
    );

    // Step past the deadline. Tokio's timer wheel may release the task a
    // tick or two after the virtual deadline, so the step includes slack;
    // the clock itself is exact (checked by the elapsed test below).
    clock.advance(Duration::from_millis(101)).await;
    tokio::task::yield_now().await;
    assert!(fired.load(Ordering::Relaxed), "timer never fired at +1s");
}

#[tokio::test]
async fn advance_unblocks_frozen_timers() {
    let clock = PausableClock::pause();
    let fired = Arc::new(AtomicBool::new(false));

    let flag = Arc::clone(&fired);
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(1)).await;
        flag.store(true, Ordering::Relaxed);
    });

    // Let the spawned task run so its sleep is registered against the
    // frozen clock before time moves (otherwise it would start counting
    // from the already-advanced instant).
    tokio::task::yield_now().await;

    // A single jump past the deadline unblocks the frozen timer. Tokio's
    // timer wheel may release the sleeper a tick past the deadline and one
    // scheduling slot after the jump, so yield until it lands.
    clock
        .advance(Duration::from_secs(1) + Duration::from_millis(100))
        .await;
    let mut yields = 0_usize;
    while !fired.load(Ordering::Relaxed) && yields < 16 {
        tokio::task::yield_now().await;
        yields += 1;
    }
    assert!(
        fired.load(Ordering::Relaxed),
        "timer never fired after advance"
    );
}

#[tokio::test]
async fn virtual_elapsed_across_advance_is_exact() {
    let clock = PausableClock::pause();

    let start = tokio::time::Instant::now();
    clock.advance(Duration::from_secs(60)).await;
    assert_eq!(start.elapsed(), Duration::from_secs(60));
}
