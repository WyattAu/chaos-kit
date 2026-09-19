# Changelog

All notable changes to this project are documented here. Format: [Keep a
Changelog](https://keepachangelog.com/) — versions follow [semver](https://semver.org).

## [0.1.1] - 2026-09-19

### Added

- **`chaos_layer_map(schedule, map)`** — the error-mapping bridge for
  services whose error type cannot absorb `ChaosError`
  (`S::Error: From<ChaosError>` does not hold). Each injected fault goes
  through `map: Fn(ChaosError) -> Result<S::Response, S::Error>`:
  - return `Ok(response)` to fold the fault into a real response — this
    is the **axum** story, where `Router` services are `Infallible` and
    an injected error can never propagate: map it to a 503-style
    response and `Router::layer` / `route_service` accept the result
    directly,
  - return `Err(error)` to propagate as your own error type — for error
    enums without a `#[from] ChaosError` variant, or types you do not
    own.
  Ships with `ChaosMakeMapLayer`, `ChaosMapLayer`, and `ChaosMapFuture`
  (`tower` feature, same as `chaos_layer`); scheduling, cloning, and
  recorder semantics match `chaos_layer`.
- Documented axum caveat: handler routes (`get(handler)`) re-materialize
  the layered service per request, restarting the call index — attach
  via `Router::route_service` for index-exact scheduling across requests
  (the docs show both patterns).
- New `axum_bridge` integration test: scripted `Fault::Error` and
  `Fault::Partition` through an axum `Router` surface as 503 responses
  carrying the chaos reason, clean indices pass through untouched, and
  the recorder counts every applied fault (axum is a dev-dependency
  only; chaos-kit itself grows no dependency).

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
