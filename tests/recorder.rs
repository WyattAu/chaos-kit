#![allow(clippy::unwrap_used, clippy::expect_used)]
//! Recorder: exact lock-free per-kind counts.

use chaos_kit::{ChaosRecorder, FaultKind};

#[test]
fn starts_at_zero() {
    let recorder = ChaosRecorder::new();
    for kind in [
        FaultKind::Latency,
        FaultKind::Error,
        FaultKind::Partition,
        FaultKind::Throttle,
        FaultKind::Cpu,
    ] {
        assert_eq!(recorder.injected(kind), 0, "{kind:?}");
    }
    assert_eq!(recorder.total(), 0);
}

#[test]
fn counts_are_exact_per_kind() {
    let recorder = ChaosRecorder::new();

    for _ in 0..3 {
        recorder.record(FaultKind::Latency);
    }
    for _ in 0..7 {
        recorder.record(FaultKind::Error);
    }
    for _ in 0..2 {
        recorder.record(FaultKind::Partition);
    }
    for _ in 0..11 {
        recorder.record(FaultKind::Throttle);
    }
    recorder.record(FaultKind::Cpu);

    assert_eq!(recorder.injected(FaultKind::Latency), 3);
    assert_eq!(recorder.injected(FaultKind::Error), 7);
    assert_eq!(recorder.injected(FaultKind::Partition), 2);
    assert_eq!(recorder.injected(FaultKind::Throttle), 11);
    assert_eq!(recorder.injected(FaultKind::Cpu), 1);
    assert_eq!(recorder.total(), 3 + 7 + 2 + 11 + 1);
}

#[test]
fn reset_zeroes_everything() {
    let recorder = ChaosRecorder::new();
    recorder.record(FaultKind::Latency);
    recorder.record(FaultKind::Cpu);
    assert_eq!(recorder.total(), 2);

    recorder.reset();
    assert_eq!(recorder.total(), 0);
    assert_eq!(recorder.injected(FaultKind::Latency), 0);
    assert_eq!(recorder.injected(FaultKind::Cpu), 0);
}

#[test]
fn clones_and_shares_count_like_metrics_kit_handles() {
    let recorder = std::sync::Arc::new(ChaosRecorder::new());

    let handles: Vec<_> = (0..4)
        .map(|_| {
            let recorder = std::sync::Arc::clone(&recorder);
            std::thread::spawn(move || {
                for _ in 0..250 {
                    recorder.record(FaultKind::Throttle);
                }
            })
        })
        .collect();
    for handle in handles {
        handle.join().expect("join");
    }

    assert_eq!(recorder.injected(FaultKind::Throttle), 1000);
}

/// The schedule's own recorder counts what its rules would inject — shared
/// across clones so a test can hold one end while a layer drives the other.
#[test]
fn schedule_recorder_is_shared_with_clones() {
    use chaos_kit::ChaosSchedule;

    let schedule = ChaosSchedule::seeded(1);
    let probe = schedule.clone();

    probe.recorder().record(FaultKind::Error);
    schedule.recorder().record(FaultKind::Error);
    schedule.recorder().record(FaultKind::Error);

    assert_eq!(probe.recorder().injected(FaultKind::Error), 3);
    assert_eq!(
        probe.recorder().injected(FaultKind::Error),
        schedule.recorder().injected(FaultKind::Error)
    );
    assert_eq!(
        ChaosSchedule::seeded(2)
            .recorder()
            .injected(FaultKind::Error),
        0
    );
}
