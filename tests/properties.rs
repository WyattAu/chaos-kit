#![allow(clippy::unwrap_used, clippy::expect_used)]
//! proptest: `fault_for` and `draw` are total — they never panic over
//! arbitrary seeds, indices, and rule sets, and they are deterministic.

use chaos_kit::{ChaosError, ChaosSchedule, Fault};
use proptest::prelude::*;
use std::time::Duration;

/// A strategy for small, chaotic rule sets: `(rule kind, param a, param b)`
/// triples that the test compiles into builder calls.
fn rule_sets() -> impl Strategy<Value = Vec<(u8, u64, u64)>> {
    prop::collection::vec((0_u8..3, any::<u64>(), any::<u64>()), 0..8)
}

fn fault_from(params: (u8, u64, u64)) -> Fault {
    match params.0 {
        0 => Fault::Latency(Duration::from_nanos(1 + params.1 % 1000)),
        1 => Fault::Error(ChaosError::Custom("proptest".into())),
        _ => Fault::Throttle {
            rate: f64::from((params.2 % 101) as u32) / 100.0,
        },
    }
}

fn build_schedule(seed: u64, rules: &[(u8, u64, u64)]) -> ChaosSchedule {
    let mut schedule = ChaosSchedule::seeded(seed);
    for &(kind, a, b) in rules {
        let fault = fault_from((kind, a, b));
        schedule = match kind {
            0 => schedule.fault_at(a, fault),
            1 => schedule.fault_every(a % 8, fault),
            _ => {
                let (from, to) = if a <= b { (a, b) } else { (b, a) };
                schedule.window(from.saturating_sub(2), to.saturating_add(2), fault)
            }
        };
    }
    schedule
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(1024))]

    #[test]
    #[allow(clippy::float_cmp)] // bit-exact determinism is the contract
    fn fault_for_is_total_and_deterministic(
        seed in any::<u64>(),
        index in any::<u64>(),
        rules in rule_sets(),
    ) {
        let schedule = build_schedule(seed, &rules);

        // Total: any query resolves, never panics, and answers twice identically.
        let first = schedule.fault_for(index);
        prop_assert_eq!(first, schedule.fault_for(index));

        // Draws are probabilities and likewise total + deterministic.
        let draw = schedule.draw(index);
        prop_assert!((0.0..1.0).contains(&draw));
        prop_assert_eq!(draw, schedule.draw(index));

        // Throttle decisions agree with the draw for any rate.
        let rate = f64::from((index % 101) as u32) / 100.0;
        prop_assert_eq!(schedule.drops(index, rate), draw < rate);
    }

    #[test]
    #[allow(clippy::float_cmp)] // bit-exact determinism is the contract
    fn same_seed_agrees_on_arbitrary_indices(
        seed in any::<u64>(),
        rules in rule_sets(),
        indices in prop::collection::vec(any::<u64>(), 1..64),
    ) {
        let a = build_schedule(seed, &rules);
        let b = build_schedule(seed, &rules);

        for index in indices {
            prop_assert_eq!(a.fault_for(index), b.fault_for(index));
            prop_assert_eq!(a.draw(index), b.draw(index));
        }
    }

    #[test]
    fn draws_of_different_seeds_eventually_diverge(
        seed in any::<u64>(),
        shift in 1_u64..=u64::MAX,
    ) {
        let a = ChaosSchedule::seeded(seed);
        let b = ChaosSchedule::seeded(seed ^ shift);

        // Somewhere in 512 indices, two independent seeds must disagree —
        // each agreement is a coin flip landing the same way.
        let diverged = (0..512).any(|index| a.drops(index, 0.5) != b.drops(index, 0.5));
        prop_assert!(diverged);
    }
}
