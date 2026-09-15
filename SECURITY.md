# Security Policy — chaos-kit

## Supported versions

| Version | Supported |
|---------|-----------|
| 0.1.x   | ✅        |

## Reporting a vulnerability

Report privately via [GitHub security advisories] for this repository, or
email **wyatt_au@protonmail.com**. Do **not** open a public issue for
security reports.

You will receive an acknowledgement within **72 hours**. Coordinated
disclosure: we ask for up to 90 days before public disclosure while a
patch ships.

## Scope notes

`chaos-kit` is **test tooling**: it injects faults into in-process call
paths to make tests deterministic. Security considerations for
integrators:

- **Do not ship chaos in production middleware.** The tower layer
  allocates per call, and `Fault::Cpu` deliberately busy-spins a core.
  Gate chaos wiring behind `#[cfg(test)]` or test-only constructors.
- `PausableClock` panics loudly outside a current-thread tokio test
  runtime — treat that panic as a signal that chaos leaked out of the
  test runtime, not a bug to suppress.
- No secrets, no network access, no filesystem access: the crate has no
  I/O surface, and the no-feature build is dependency-free.
- `#![forbid(unsafe_code)]` — no unsafe blocks exist in this crate.

[GitHub security advisories]:
    https://github.com/WyattAu/chaos-kit/security/advisories/new
