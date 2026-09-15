# Changelog

All notable changes to this project are documented here. Format: [Keep a
Changelog](https://keepachangelog.com/) — versions follow [semver](https://semver.org).

## [0.1.0] - 2026-09-15

### Added

- `Fault` model: `Latency`, `Error(ChaosError)`, `Partition{direction,
  for_duration}`, `Throttle{rate}`, `Cpu` — with `Direction` (ingress /
  egress) and `ChaosError::{Unavailable, Timeout, Custom}` implementing
  `std::error::Error`.
- `ChaosSchedule`: seeded (inline SplitMix64, no RNG dependency) and
  deterministic — `fault_at`, `fault_every`, `window` builders,
  `fault_for(index)` as a pure function of index + seed, `draw(index)` /
  `drops(index, rate)` for seeded probabilistic decisions.
- `tower` feature: `ChaosLayer` service wrapper + `chaos_layer(schedule)`
  `tower::Layer`, applying faults per call index; injected errors surface
  through `S::Error: From<ChaosError>`.
- `tokio-test` feature: `PausableClock` — frozen tokio time with loud
  panics outside a current-thread runtime, and single-yield `advance`.
- `ChaosRecorder`: lock-free relaxed-atomic per-`FaultKind` injection
  counters shared through schedule clones.
- Criterion benches (schedule lookup, seeded draw, empty-schedule layer
  overhead), hermetic integration tests (tokio virtual-time latency,
  partition, seeded throttle ratio bounds), proptest totality of
  `fault_for` / `draw`.
