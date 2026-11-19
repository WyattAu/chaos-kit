# PATTERN_ADOPTION

chaos-kit declares its relationship to the WyattAu pattern canon
(pattern-standards). Format: references/adoption-format.md; validated by
scripts/adoption-check.py.

| Pattern | Status | Evidence |
|---|---|---|
| T13 | adopted | src/schedule.rs:120 (fault_for is a pure lookup: same seed plus same call index yields the same fault) and src/tower_layer.rs:191 (the tower layer is the imperative shell applying it) |
| T36 | partial | src/schedule.rs:68 (seeded schedule: fault as a pure function of call index and seed) and src/clock.rs:38 (PausableClock freezes tokio time); middleware seams only, no disk or network-stack abstraction and no task-interleaving scheduler |
| P29 | adopted | src/tower_layer.rs:70 (chaos_layer injects the five FaultKind variants, src/fault.rs:54, into any tower service) |
| P30 | n/a | ChaosRecorder (src/recorder.rs:29) counts injected faults per kind for post-scenario assertions; it is not an always-on event recorder |
| P64 | adopted | tests/properties.rs:41 (fault_for total and deterministic, same seed agrees across indices, different seeds diverge; nightly enlarged soak with PROPTEST_CASES=4096 not yet scheduled, target 2027-Q1) |
| P101 | n/a | in-process test library by design; production experiment scheduling is outside its scope (README.md:28) |
