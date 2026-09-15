#![allow(clippy::unwrap_used, clippy::expect_used)]
//! Schedule determinism and builder semantics: the pure-function contract.

use chaos_kit::{ChaosError, ChaosSchedule, Direction, Fault, FaultKind};
use std::time::Duration;

fn latency(ms: u64) -> Fault {
    Fault::Latency(Duration::from_millis(ms))
}

fn timeout() -> Fault {
    Fault::Error(ChaosError::Timeout)
}

/// Same seed + same rules => the identical fault sequence over 1000 calls.
#[test]
fn same_seed_reproduces_the_fault_sequence_over_1000_indices() {
    let build = || {
        ChaosSchedule::seeded(1234)
            .fault_at(7, timeout())
            .fault_every(3, latency(10))
            .window(900, 999, Fault::Throttle { rate: 0.5 })
    };
    let a = build();
    let b = build();

    assert_eq!(a.seed(), b.seed());
    for index in 0..1000 {
        assert_eq!(a.fault_for(index), b.fault_for(index), "index {index}");
        // And a repeated query on the same schedule agrees:
        assert_eq!(a.fault_for(index), a.fault_for(index), "index {index}");
    }
}

/// Same seed => identical seeded draws (throttle decisions) over 1000 calls.
#[test]
#[allow(clippy::float_cmp)] // bit-exact determinism is the contract
fn same_seed_reproduces_throttle_decisions_over_1000_indices() {
    let a = ChaosSchedule::seeded(0xFEED);
    let b = ChaosSchedule::seeded(0xFEED);

    for index in 0..1000 {
        assert_eq!(a.draw(index), b.draw(index), "index {index}");
        assert_eq!(a.drops(index, 0.5), b.drops(index, 0.5), "index {index}");
    }
}

/// Different seeds must diverge quickly: at least one differing throttle
/// decision within 100 indices. (P(all 100 agree) ~ 2^-100 for rate 0.5.)
#[test]
fn different_seed_diverges_within_100_indices() {
    let a = ChaosSchedule::seeded(1);
    let b = ChaosSchedule::seeded(2);

    assert_ne!(a.seed(), b.seed());
    let diverged = (0..100).any(|index| a.drops(index, 0.5) != b.drops(index, 0.5));
    assert!(diverged, "seeds 1 and 2 agreed on every drop in 0..100");
}

#[test]
fn fault_at_fires_exactly_once() {
    let schedule = ChaosSchedule::seeded(9).fault_at(5, timeout());

    assert_eq!(schedule.fault_for(4), None);
    assert_eq!(schedule.fault_for(5), Some(&timeout()));
    assert_eq!(schedule.fault_for(6), None);
}

#[test]
fn fault_every_fires_on_multiples_and_never_at_a_zero_period() {
    let schedule = ChaosSchedule::seeded(9).fault_every(4, latency(1));

    assert_eq!(schedule.fault_for(0), Some(&latency(1)));
    assert_eq!(schedule.fault_for(3), None);
    assert_eq!(schedule.fault_for(4), Some(&latency(1)));
    assert_eq!(schedule.fault_for(8), Some(&latency(1)));

    // A zero-period rule is total but inert — it never panics.
    let zero = ChaosSchedule::seeded(9).fault_every(0, timeout());
    for index in [0, 1, 7, 1_000_000] {
        assert_eq!(zero.fault_for(index), None, "index {index}");
    }
}

#[test]
fn window_is_inclusive_and_empty_when_reversed() {
    let schedule = ChaosSchedule::seeded(9).window(10, 12, timeout());

    assert_eq!(schedule.fault_for(9), None);
    assert_eq!(schedule.fault_for(10), Some(&timeout()));
    assert_eq!(schedule.fault_for(11), Some(&timeout()));
    assert_eq!(schedule.fault_for(12), Some(&timeout()));
    assert_eq!(schedule.fault_for(13), None);

    let reversed = ChaosSchedule::seeded(9).window(12, 10, timeout());
    assert_eq!(reversed.fault_for(11), None);
    assert_eq!(reversed.fault_for(12), None);
}

#[test]
fn later_rules_override_earlier_ones() {
    let schedule = ChaosSchedule::seeded(9)
        .fault_every(2, latency(1))
        .fault_at(4, timeout());

    // Index 4 matches both rules; the later registration wins.
    assert_eq!(schedule.fault_for(4), Some(&timeout()));
    assert_eq!(schedule.fault_for(2), Some(&latency(1)));
}

#[test]
fn draws_stay_in_the_unit_interval() {
    for seed in [0_u64, 1, 42, u64::MAX] {
        let schedule = ChaosSchedule::seeded(seed);
        for index in 0..1000 {
            let draw = schedule.draw(index);
            assert!(
                (0.0..1.0).contains(&draw),
                "seed {seed} index {index} drew {draw}"
            );
        }
    }
}

#[test]
fn throttle_rates_saturate_outside_the_unit_interval() {
    let schedule = ChaosSchedule::seeded(5).fault_every(1, Fault::Throttle { rate: 0.5 });

    assert!((0..1000).all(|index| schedule.drops(index, 2.0)));
    assert!((0..1000).all(|index| !schedule.drops(index, -1.0)));
    assert!((0..1000).all(|index| !schedule.drops(index, 0.0)));
}

#[test]
fn empty_schedule_is_clean_everywhere() {
    let schedule = ChaosSchedule::seeded(u64::MAX);
    for index in [0, 1, 1_000, u64::MAX] {
        assert_eq!(schedule.fault_for(index), None);
    }
    assert_eq!(schedule.recorder().total(), 0);
}

/// The recorder travels with schedule clones — counts stay shared.
#[test]
fn cloned_schedules_share_the_recorder() {
    let original = ChaosSchedule::seeded(3);
    let clone = original.clone();
    clone.recorder().record(FaultKind::Partition);

    assert_eq!(original.recorder().injected(FaultKind::Partition), 1);
}

/// Fault identity survives the data model: partition direction and
/// duration are assertable metadata.
#[test]
fn partition_faults_carry_direction_metadata() {
    let ingress = Fault::Partition {
        direction: Direction::Ingress,
        for_duration: Duration::from_secs(30),
    };
    let egress = Fault::Partition {
        direction: Direction::Egress,
        for_duration: Duration::from_secs(30),
    };

    assert_ne!(ingress, egress);
    assert_eq!(ingress.kind(), egress.kind());

    let schedule = ChaosSchedule::seeded(1).fault_at(0, ingress.clone());
    assert_eq!(schedule.fault_for(0), Some(&ingress));
}
