//! Deterministic chaos scheduling.
//!
//! A [`ChaosSchedule`] maps call indices to [`Fault`]s as a pure function of
//! the index and the seed. Nothing in a schedule consults wall time, threads,
//! or global state: same seed + same call sequence = same chaos, forever.

use std::sync::Arc;

use crate::fault::{Fault, FaultKind};
use crate::recorder::ChaosRecorder;

/// The `SplitMix64` golden-gamma constant (Steele & Lea, public domain).
const GOLDEN_GAMMA: u64 = 0x9E37_79B9_7F4A_7C15;

/// One round of `SplitMix64` — inlined so the crate has no RNG dependency and
/// the output stream is frozen for the life of the crate.
fn splitmix64(state: &mut u64) -> u64 {
    *state = state.wrapping_add(GOLDEN_GAMMA);
    let mut z = *state;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

/// One scheduling rule; later rules take precedence over earlier ones.
#[derive(Debug, Clone)]
enum Rule {
    /// Fire exactly once, at this call index.
    At(u64, Fault),
    /// Fire on every `n`-th call index (`n == 0` never fires).
    Every(u64, Fault),
    /// Fire on the inclusive range `[from, to]` (empty when `from > to`).
    Window(u64, u64, Fault),
}

impl Rule {
    fn matches(&self, call_index: u64) -> bool {
        match self {
            Self::At(at, _) => *at == call_index,
            Self::Every(n, _) => *n != 0 && call_index % n == 0,
            Self::Window(from, to, _) => *from <= call_index && call_index <= *to,
        }
    }

    fn fault(&self) -> &Fault {
        match self {
            Self::At(_, fault) | Self::Every(_, fault) | Self::Window(_, _, fault) => fault,
        }
    }
}

/// A deterministic fault schedule.
///
/// # Determinism
///
/// [`ChaosSchedule::fault_for`] is a pure function of the call index and
/// [`ChaosSchedule::seed`]; [`ChaosSchedule::draw`] (and therefore every
/// [`Fault::Throttle`] decision, via [`ChaosSchedule::drops`]) is a pure
/// function of the seed and the index. The same seed with the same call
/// sequence reproduces the exact same chaos on every run, on every machine.
///
/// # Recorder
///
/// Every schedule carries a lock-free [`ChaosRecorder`]; clones share it.
/// The tower layer records injected faults there so tests can assert
/// `schedule.recorder().injected(FaultKind::Latency) == 3`.
#[derive(Debug, Clone)]
pub struct ChaosSchedule {
    seed: u64,
    rules: Vec<Rule>,
    recorder: Arc<ChaosRecorder>,
}

impl ChaosSchedule {
    /// A schedule with the given seed and no rules (nothing fires).
    ///
    /// The seed drives every probabilistic decision ([`Fault::Throttle`]);
    /// explicit rules are layered on with the builder methods.
    #[must_use]
    pub fn seeded(seed: u64) -> Self {
        Self {
            seed,
            rules: Vec::new(),
            recorder: Arc::new(ChaosRecorder::new()),
        }
    }

    /// Schedule `fault` at exactly `call_index`.
    #[must_use]
    pub fn fault_at(mut self, call_index: u64, fault: Fault) -> Self {
        self.rules.push(Rule::At(call_index, fault));
        self
    }

    /// Schedule `fault` on every `n`-th call (indices divisible by `n`).
    ///
    /// `n == 0` never fires — a zero-period rule is a programming mistake
    /// and silently scheduling every call would hide it; keep the schedule
    /// total and panic-free.
    #[must_use]
    pub fn fault_every(mut self, n: u64, fault: Fault) -> Self {
        self.rules.push(Rule::Every(n, fault));
        self
    }

    /// Schedule `fault` on the inclusive call-index range
    /// `[from_call, to_call]`. A range with `from_call > to_call` never
    /// fires.
    #[must_use]
    pub fn window(mut self, from_call: u64, to_call: u64, fault: Fault) -> Self {
        self.rules.push(Rule::Window(from_call, to_call, fault));
        self
    }

    /// The fault scheduled for `call_index`, if any.
    ///
    /// Pure function of the index (plus the immutable rule set). When
    /// several rules match, the most recently added one wins.
    #[must_use]
    pub fn fault_for(&self, call_index: u64) -> Option<&Fault> {
        self.rules
            .iter()
            .rev()
            .find(|rule| rule.matches(call_index))
            .map(Rule::fault)
    }

    /// A deterministic pseudo-random draw in `[0, 1)` for `call_index`,
    /// derived from the schedule seed via `SplitMix64`. Pure function of
    /// (seed, index).
    // Precision loss is inherent and fine here: 53 mantissa bits make the
    // draw a uniform probability, not an identity-preserving conversion.
    #[allow(clippy::cast_precision_loss)]
    #[must_use]
    pub fn draw(&self, call_index: u64) -> f64 {
        // Seed the generator per-index (golden-ratio stride decorrelates
        // neighbouring indices), then warm up one round so low-seed /
        // low-index inputs do not land on SplitMix64's weak first draws.
        let mut state = self.seed ^ call_index.wrapping_mul(GOLDEN_GAMMA);
        let _ = splitmix64(&mut state);
        let z = splitmix64(&mut state);
        // 53 significant bits — the f64 mantissa width — scaled into [0, 1).
        (z >> 11) as f64 * (1.0 / (1_u64 << 53) as f64)
    }

    /// Whether a [`Fault::Throttle`] with admission `rate` drops
    /// `call_index` — pure function of (seed, index, rate).
    #[must_use]
    pub fn drops(&self, call_index: u64, rate: f64) -> bool {
        self.draw(call_index) < rate
    }

    /// The seed this schedule was built with.
    #[must_use]
    pub fn seed(&self) -> u64 {
        self.seed
    }

    /// The shared lock-free injection recorder for this schedule.
    #[must_use]
    pub fn recorder(&self) -> &ChaosRecorder {
        &self.recorder
    }

    /// Record an injected fault of `kind` into the shared recorder.
    ///
    /// The tower layer calls this for every fault it applies; custom
    /// injectors driving faults by hand can use it to keep the counts true.
    pub fn record(&self, kind: FaultKind) {
        self.recorder.record(kind);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fault::{ChaosError, Direction};
    use std::time::Duration;

    #[test]
    fn splitmix64_matches_reference_stream() {
        // Reference stream for state 0 (SplitMix64, Steele & Lea; verified
        // against the canonical reference implementation).
        let mut state = 0_u64;
        assert_eq!(splitmix64(&mut state), 0xE220_A839_7B1D_CDAF);
        assert_eq!(splitmix64(&mut state), 0x6E78_9E6A_A1B9_65F4);
        assert_eq!(splitmix64(&mut state), 0x06C4_5D18_8009_454F);
    }

    #[test]
    #[allow(clippy::float_cmp)] // bit-exact determinism is the contract
    fn draw_is_splitmix64_of_seeded_state() {
        let schedule = ChaosSchedule::seeded(0);
        // state = 0 ^ 0 = 0: the warm-up round consumes the paper's first
        // output, the sampling round the second.
        assert_eq!(schedule.draw(0), 0.4315_2799_7048_5099_7);
    }

    #[test]
    fn fault_kind_maps_every_variant() {
        let faults = [
            Fault::Latency(Duration::from_millis(1)),
            Fault::Error(ChaosError::Custom("boom".into())),
            Fault::Partition {
                direction: Direction::Ingress,
                for_duration: Duration::from_secs(1),
            },
            Fault::Throttle { rate: 0.5 },
            Fault::Cpu(Duration::from_nanos(1)),
        ];
        let kinds = [
            FaultKind::Latency,
            FaultKind::Error,
            FaultKind::Partition,
            FaultKind::Throttle,
            FaultKind::Cpu,
        ];
        for (fault, kind) in faults.iter().zip(kinds) {
            assert_eq!(fault.kind(), kind);
        }
    }
}
