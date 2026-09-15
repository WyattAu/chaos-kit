//! The fault model: what chaos can be injected, and the errors it produces.

use std::borrow::Cow;
use std::fmt;
use std::time::Duration;

/// Which side of a service boundary a [`Fault::Partition`] severs.
///
/// In-process, both directions short-circuit *before* the wrapped service is
/// called. The direction records which failure mode you are modelling:
/// requests that cannot arrive, or responses that cannot leave.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Direction {
    /// Requests cannot reach the wrapped service — the call is rejected at
    /// the door.
    Ingress,
    /// Responses cannot leave the call path — the call is cut before the
    /// upstream dependency would be reached.
    Egress,
}

/// The error chaos-kit injects instead of a real response.
///
/// [`Fault::Error`] injects the exact variant you script; faulted
/// [`Fault::Partition`] and dropped [`Fault::Throttle`] calls surface as
/// [`ChaosError::Unavailable`]. `From<ChaosError>` lets downstream
/// middleware absorb it into richer error types.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChaosError {
    /// A modelled dependency outage — the 503-shaped fault.
    Unavailable,
    /// A modelled timeout — the dependency never answered in time.
    Timeout,
    /// A free-form scripted failure message.
    Custom(Cow<'static, str>),
}

impl fmt::Display for ChaosError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unavailable => write!(f, "chaos: dependency unavailable"),
            Self::Timeout => write!(f, "chaos: dependency timed out"),
            Self::Custom(msg) => write!(f, "chaos: {msg}"),
        }
    }
}

impl std::error::Error for ChaosError {}

/// The kind of a [`Fault`], without its payload.
///
/// Used as the key for [`ChaosRecorder`](crate::ChaosRecorder) counters.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FaultKind {
    /// Delay before forwarding — see [`Fault::Latency`].
    Latency,
    /// Scripted error response — see [`Fault::Error`].
    Error,
    /// Severed call path — see [`Fault::Partition`].
    Partition,
    /// Probabilistic drop — see [`Fault::Throttle`].
    Throttle,
    /// CPU starvation busy-spin — see [`Fault::Cpu`].
    Cpu,
}

/// A single fault a [`ChaosSchedule`](crate::ChaosSchedule) can inject.
///
/// Every variant is applied per call index by the tower layer (feature
/// `tower`); the mapping is:
///
/// | Variant | Effect on the call |
/// |---|---|
/// | [`Fault::Latency`] | sleep `for_duration` (tokio time — freezable), then forward |
/// | [`Fault::Error`] | return the scripted [`ChaosError`]; inner service never called |
/// | [`Fault::Partition`] | sever the call immediately ([`ChaosError::Unavailable`]); inner service never called |
/// | [`Fault::Throttle`] | seeded draw for this index; drop ([`ChaosError::Unavailable`]) when it loses the race |
/// | [`Fault::Cpu`] | busy-spin one core for `for_duration`, then forward |
#[derive(Debug, Clone, PartialEq)]
pub enum Fault {
    /// Inject `Duration` of latency before the request is forwarded.
    Latency(Duration),
    /// Return this [`ChaosError`] instead of calling the wrapped service.
    Error(ChaosError),
    /// Model a severed call path. `for_duration` documents the intended
    /// outage length (for logs and assertions); injection is per call index
    /// and does not track wall time, which keeps the schedule deterministic.
    Partition {
        /// Which side of the boundary is severed.
        direction: Direction,
        /// Documented outage length (advisory metadata).
        for_duration: Duration,
    },
    /// Probabilistically drop calls, admitting a `rate` fraction in `[0, 1]`.
    /// Values outside `[0, 1]` saturate: `rate > 1` drops every call,
    /// `rate <= 0` drops none. The per-index draw is derived from the
    /// schedule seed, so the same seed reproduces the exact same drop
    /// pattern.
    Throttle {
        /// Admission rate in `[0, 1]` (saturating outside that range).
        rate: f64,
    },
    /// Starve the runtime of a core: busy-spin for `Duration` before
    /// forwarding. This burns a real core on the calling thread — it is a
    /// test-only probe of CPU contention, not a production middleware
    /// behavior. Keep the duration small (microseconds) in tests.
    Cpu(Duration),
}

impl Fault {
    /// The [`FaultKind`] of this fault, for recorder lookups.
    #[must_use]
    pub fn kind(&self) -> FaultKind {
        match self {
            Self::Latency(_) => FaultKind::Latency,
            Self::Error(_) => FaultKind::Error,
            Self::Partition { .. } => FaultKind::Partition,
            Self::Throttle { .. } => FaultKind::Throttle,
            Self::Cpu(_) => FaultKind::Cpu,
        }
    }
}
