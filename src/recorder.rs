//! Lock-free injection counters, in the metrics-kit tradition.
//!
//! One `AtomicUsize` per [`FaultKind`], touched with relaxed orderings: the
//! hot path never takes a lock, and tests read totals after joining.

use std::sync::atomic::{AtomicUsize, Ordering};

use crate::fault::FaultKind;

/// Counts of injected faults per [`FaultKind`].
///
/// Shared between a [`ChaosSchedule`](crate::ChaosSchedule) and every clone
/// of it, and between a [`ChaosLayer`](crate::ChaosLayer) and its clones —
/// hand the recorder around and assert on it after the scenario runs:
///
/// ```
/// use chaos_kit::{ChaosRecorder, FaultKind};
///
/// let recorder = ChaosRecorder::new();
/// recorder.record(FaultKind::Latency);
/// recorder.record(FaultKind::Latency);
/// recorder.record(FaultKind::Throttle);
///
/// assert_eq!(recorder.injected(FaultKind::Latency), 2);
/// assert_eq!(recorder.injected(FaultKind::Throttle), 1);
/// assert_eq!(recorder.total(), 3);
/// ```
#[derive(Debug, Default)]
pub struct ChaosRecorder {
    latency: AtomicUsize,
    error: AtomicUsize,
    partition: AtomicUsize,
    throttle: AtomicUsize,
    cpu: AtomicUsize,
}

impl ChaosRecorder {
    /// A recorder with all counters at zero.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Count one injected fault of `kind`.
    pub fn record(&self, kind: FaultKind) {
        match kind {
            FaultKind::Latency => self.latency.fetch_add(1, Ordering::Relaxed),
            FaultKind::Error => self.error.fetch_add(1, Ordering::Relaxed),
            FaultKind::Partition => self.partition.fetch_add(1, Ordering::Relaxed),
            FaultKind::Throttle => self.throttle.fetch_add(1, Ordering::Relaxed),
            FaultKind::Cpu => self.cpu.fetch_add(1, Ordering::Relaxed),
        };
    }

    /// How many faults of `kind` have been injected.
    #[must_use]
    pub fn injected(&self, kind: FaultKind) -> usize {
        match kind {
            FaultKind::Latency => self.latency.load(Ordering::Relaxed),
            FaultKind::Error => self.error.load(Ordering::Relaxed),
            FaultKind::Partition => self.partition.load(Ordering::Relaxed),
            FaultKind::Throttle => self.throttle.load(Ordering::Relaxed),
            FaultKind::Cpu => self.cpu.load(Ordering::Relaxed),
        }
    }

    /// How many faults have been injected, all kinds together.
    #[must_use]
    pub fn total(&self) -> usize {
        self.injected(FaultKind::Latency)
            + self.injected(FaultKind::Error)
            + self.injected(FaultKind::Partition)
            + self.injected(FaultKind::Throttle)
            + self.injected(FaultKind::Cpu)
    }

    /// Zero every counter — handy between scenarios.
    pub fn reset(&self) {
        for kind in [
            FaultKind::Latency,
            FaultKind::Error,
            FaultKind::Partition,
            FaultKind::Throttle,
            FaultKind::Cpu,
        ] {
            let counter = match kind {
                FaultKind::Latency => &self.latency,
                FaultKind::Error => &self.error,
                FaultKind::Partition => &self.partition,
                FaultKind::Throttle => &self.throttle,
                FaultKind::Cpu => &self.cpu,
            };
            counter.store(0, Ordering::Relaxed);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counters_round_trip() {
        let recorder = ChaosRecorder::new();
        assert_eq!(recorder.total(), 0);
        recorder.record(FaultKind::Cpu);
        recorder.record(FaultKind::Cpu);
        recorder.record(FaultKind::Partition);
        assert_eq!(recorder.injected(FaultKind::Cpu), 2);
        assert_eq!(recorder.injected(FaultKind::Partition), 1);
        assert_eq!(recorder.total(), 3);
        recorder.reset();
        assert_eq!(recorder.total(), 0);
    }
}
