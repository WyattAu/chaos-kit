//! Test-clock control: freeze and step tokio time so latency chaos is
//! instant and exact.
//!
//! ⚠️ **Test-runtime only.** Tokio only supports pausing time on the
//! current-thread runtime. Every method here **panics loudly** when called
//! outside a `#[tokio::test]` (or another current-thread runtime with time
//! enabled) — that failure is the feature: it catches "I wired chaos into
//! a production path" mistakes at the first call.

use std::time::Duration;

/// A frozen tokio clock handle: pause once, then advance deterministically.
///
/// # Panics — loudly, by design
///
/// [`PausableClock::pause`] panics when called
///
/// - outside any tokio runtime (a plain `#[test]`, a helper thread, or
///   before the runtime starts), or
/// - inside a multi-thread runtime (`#[tokio::test(flavor = "multi_thread")]`),
///   where tokio cannot freeze time.
///
/// Use the default `#[tokio::test]` — it is a current-thread runtime with
/// time enabled, exactly what chaos tests want.
///
/// # Example
///
/// ```rust,no_run
/// // Inside #[tokio::test]:
/// # async fn scenario() {
/// let clock = chaos_kit::PausableClock::pause();
///
/// // A chaos latency of 1h resolves instantly in wall time:
/// clock.advance(std::time::Duration::from_secs(60 * 60)).await;
/// # }
/// ```
#[derive(Debug, Clone, Copy)]
pub struct PausableClock {
    _private: (),
}

impl PausableClock {
    /// Freeze tokio time: timers only fire when you [`PausableClock::advance`]
    /// (or when the runtime would otherwise idle on a paused timer).
    ///
    /// # Panics
    ///
    /// Panics with a clear message when not called from inside a
    /// current-thread tokio runtime with time enabled — see the type docs.
    #[allow(clippy::panic)] // deliberate: test scaffolding must fail fast, loudly
    #[must_use]
    pub fn pause() -> Self {
        let handle = tokio::runtime::Handle::try_current().unwrap_or_else(|_| {
            panic!(
                "chaos-kit: PausableClock::pause requires a tokio runtime with time enabled — \
                 call it inside #[tokio::test] (current-thread), not from a plain #[test] \
                 or a helper thread"
            )
        });
        assert_eq!(
            handle.runtime_flavor(),
            tokio::runtime::RuntimeFlavor::CurrentThread,
            "chaos-kit: tokio cannot freeze time on a multi-thread runtime — \
             use the default #[tokio::test] (current-thread), not \
             #[tokio::test(flavor = \"multi_thread\")]"
        );
        tokio::time::pause();
        Self { _private: () }
    }

    /// Advance frozen time by `duration`, then yield once so tasks woken by
    /// the jump usually get to run before this returns. (Tokio's timer wheel
    /// may release sleepers a tick past the deadline; loop on your
    /// condition for hard guarantees.)
    ///
    /// # Determinism tip
    ///
    /// After `tokio::spawn`ing a task whose sleep/latency you intend to
    /// advance past, `tokio::task::yield_now().await` once so the task
    /// actually registers its timer against the frozen clock first. A timer
    /// registered *after* an advance starts counting from the new instant.
    ///
    /// # Panics
    ///
    /// Panics when time is not frozen (call [`PausableClock::pause`] first)
    /// — tokio's own message names the runtime problem.
    pub async fn advance(&self, duration: Duration) {
        tokio::time::advance(duration).await;
        tokio::task::yield_now().await;
    }
}
