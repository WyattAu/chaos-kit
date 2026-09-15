//! Deterministic fault injection for Rust tests.
//!
//! `chaos-kit` makes in-process chaos **reproducible**: latency, errors,
//! network partitions, throttling, and CPU starvation, scheduled as a pure
//! function of a seed and the call index. Same seed + same call sequence =
//! same chaos — on every run, on every machine, in CI and in `rr`.
//!
//! # Design
//!
//! - **Schedules are deterministic.** [`ChaosSchedule`] maps call indices
//!   to [`Fault`]s with explicit rules plus a SplitMix64-seeded stream
//!   (inlined — no RNG dependency) for probabilistic faults like
//!   [`Fault::Throttle`].
//! - **Injection happens at the middleware layer.** With the `tower`
//!   feature, [`chaos_layer`] wraps any `tower::Service` and applies the
//!   scheduled fault per request. Nothing else is touched.
//! - **The clock is yours.** With the `tokio-test` feature,
//!   [`PausableClock`] freezes tokio time so a one-hour latency fault
//!   resolves instantly and asserts exactly.
//! - **Injections are counted.** [`ChaosRecorder`] keeps lock-free
//!   per-kind counters for post-scenario assertions.
//!
//! # Scope — in-process only, on purpose
//!
//! chaos-kit injects at the *application middleware* layer: it makes
//! **in-process tests** deterministic. It deliberately does not schedule
//! real infrastructure — for VM/container-level chaos use the estate's
//! `EvergreenShims` `chaos-shim`, and for network partitions between
//! processes use [toxiproxy]. This crate will never grow a scheduler of
//! pods, containers, or network namespaces.
//!
//! [toxiproxy]: https://github.com/Shopify/toxiproxy
//!
//! # Example
//!
//! ```
//! use chaos_kit::{ChaosError, ChaosSchedule, Fault};
//! use std::time::Duration;
//!
//! let schedule = ChaosSchedule::seeded(0xC0FFEE)
//!     .fault_at(2, Fault::Latency(Duration::from_millis(500)))
//!     .fault_every(5, Fault::Error(ChaosError::Timeout));
//!
//! // Pure function of index + seed — the same call sequence always sees
//! // the same chaos:
//! assert_eq!(
//!     schedule.fault_for(2),
//!     Some(&Fault::Latency(Duration::from_millis(500)))
//! );
//! assert_eq!(schedule.fault_for(5), Some(&Fault::Error(ChaosError::Timeout)));
//! assert_eq!(schedule.fault_for(6), None);
//! ```
//!
//! # Feature flags
//!
//! | Feature | Default | Description |
//! |---|---|---|
//! | `tower` | yes | [`chaos_layer`] / [`ChaosLayer`] middleware for `tower::Service` |
//! | `tokio-test` | yes | [`PausableClock`] for frozen tokio time in tests |
//!
//! The default build with no features is dependency-free: schedules,
//! faults, and the recorder compile anywhere.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

mod fault;
mod recorder;
mod schedule;

pub use fault::{ChaosError, Direction, Fault, FaultKind};
pub use recorder::ChaosRecorder;
pub use schedule::ChaosSchedule;

#[cfg(feature = "tower")]
mod tower_layer;
#[cfg(feature = "tower")]
pub use tower_layer::{chaos_layer, ChaosFuture, ChaosLayer, ChaosMakeLayer};

#[cfg(feature = "tokio-test")]
mod clock;
#[cfg(feature = "tokio-test")]
pub use clock::PausableClock;
