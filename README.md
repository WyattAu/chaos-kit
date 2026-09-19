# chaos-kit

Deterministic fault injection for Rust tests — latency, errors, partitions,
throttling, and CPU starvation with seeded scheduling. The estate's
in-process chaos tool: **same seed + same call sequence = same chaos**, on
every run, on every machine.

- **Deterministic schedules**: `ChaosSchedule` maps call indices to faults
  as a pure function of index + seed. SplitMix64 is inlined — no RNG
  dependency, frozen stream.
- **Fault model**: latency (tokio virtual time), scripted errors,
  ingress/egress partitions, seeded throttling, CPU-starvation busy-spin.
- **Tower middleware** (default): `chaos_layer(schedule)` wraps any
  `tower::Service`; faults apply per request index, errors surface through
  `S::Error: From<ChaosError>`. For services whose error type cannot
  absorb `ChaosError` — axum's `Infallible` routers, foreign error enums —
  `chaos_layer_map(schedule, map)` hands each fault to your closure:
  map it to a response (the axum bridge) or your own error.
- **Frozen clock** (default): `PausableClock` pauses tokio time so a
  one-hour latency fault is instant and exact — with loud panics when used
  outside a `#[tokio::test]`.
- **Lock-free recorder**: per-`FaultKind` relaxed-atomic counters for
  exact post-scenario assertions.
- **`#![forbid(unsafe_code)]`, `#![deny(missing_docs)]`**, clippy
  `unwrap_used`/`expect_used`/`panic`/`indexing_slicing` denied.

## Scope — in-process only, on purpose

chaos-kit injects at the *application middleware* layer and makes
**in-process tests** deterministic. It will never schedule real
infrastructure:

- VM/container-level chaos → EvergreenShims' `chaos-shim`
- Network partitions between processes → [toxiproxy]

[toxiproxy]: https://github.com/Shopify/toxiproxy

## Install

```toml
[dev-dependencies]
chaos-kit = "0.1"
```

## Example

```rust
use chaos_kit::{ChaosError, ChaosSchedule, Fault};
use std::time::Duration;

let schedule = ChaosSchedule::seeded(0xC0FFEE)
    .fault_at(2, Fault::Latency(Duration::from_millis(500)))
    .window(10, 20, Fault::Partition {
        direction: chaos_kit::Direction::Egress,
        for_duration: Duration::from_secs(30),
    })
    .fault_every(5, Fault::Error(ChaosError::Timeout));

// Pure function of index + seed:
assert_eq!(
    schedule.fault_for(2),
    Some(&Fault::Latency(Duration::from_millis(500)))
);
assert_eq!(schedule.fault_for(5), Some(&Fault::Error(ChaosError::Timeout)));
assert_eq!(schedule.fault_for(6), None);

// Seeded probabilities are deterministic too:
assert_eq!(schedule.draw(42), schedule.draw(42));
```

Wrap a tower service (see the `chaos_layer` docs for a full listing):

```rust,ignore
let mut service = chaos_layer(schedule).layer(my_service);
// request #2 sleeps 500ms of tokio virtual time, then forwards;
// request #5 returns ChaosError::Timeout; ...
```

Freeze the clock so latency chaos costs zero wall time:

```rust,ignore
// inside #[tokio::test]:
let clock = PausableClock::pause();
clock.advance(Duration::from_secs(60 * 60)).await;
```

Assert on what was injected:

```rust,ignore
assert_eq!(schedule.recorder().injected(FaultKind::Latency), 3);
```

## Performance

Measured with criterion on the committed bench suite (`cargo bench`);
see `benches/chaos_bench.rs`:

| Operation | Path | Cost model |
|---|---|---|
| `fault_for(index)` | schedule | reverse rule scan, no allocation |
| `draw(index)` | schedule | 2 SplitMix64 rounds, no allocation |
| `ChaosLayer` call, empty schedule | hot | index bump + rule scan + forward |
| latency/forward faults | hot | one boxed `Sleep` + boxed inner future |

This is test tooling, not a production hot path — the layer trades a
small allocation per call for a single code path.

## Feature flags

| Feature | Default | Description |
|---|---|---|
| `tower` | yes | `chaos_layer(schedule)` / `chaos_layer_map(schedule, map)` → `tower::Layer`; `ChaosLayer` service |
| `tokio-test` | yes | `PausableClock` — frozen tokio time for tests |

The no-feature build is dependency-free (faults, schedules, recorder).

## License

Licensed under either of [Apache-2.0](LICENSE-APACHE) or [MIT](LICENSE-MIT)
at your option.
