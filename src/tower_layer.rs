//! The tower middleware integration: one [`Fault`] applied per call index.
//!
//! Only the *call path* is chaos-gated: [`ChaosLayer`] sits between your
//! test's code and the wrapped service, consults the schedule with the
//! per-layer call counter, and applies the fault (or forwards). It never
//! touches networks, containers, or VMs — for that, use the estate's
//! infrastructure-level tooling (see the crate docs).
//!
//! Note this is **test tooling**: the layer allocates one small future
//! per call and [`Fault::Cpu`] deliberately burns a core. Do not ship it
//! in production middleware.
//!
//! Two error contracts are supported: [`ChaosLayer`] (from
//! [`chaos_layer`]) injects through `S::Error: From<ChaosError>`, while
//! [`chaos_layer_map`] hands each injected fault to a caller-supplied
//! closure — the bridge for services whose error type cannot absorb
//! [`ChaosError`] (axum's `Infallible` services, foreign error enums).

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use pin_project_lite::pin_project;
use tokio::time::Sleep;
use tower::{Layer, Service};

use crate::fault::{ChaosError, Fault, FaultKind};
use crate::recorder::ChaosRecorder;
use crate::schedule::ChaosSchedule;

/// A `tower::Layer` that wraps any service in a [`ChaosSchedule`].
///
/// Produced by [`chaos_layer`]; composes with `Router::layer`,
/// `ServiceBuilder`, and friends. Cloning it shares the schedule (and its
/// recorder).
#[derive(Debug, Clone)]
pub struct ChaosMakeLayer {
    schedule: Arc<ChaosSchedule>,
}

impl<S> Layer<S> for ChaosMakeLayer {
    type Service = ChaosLayer<S>;

    fn layer(&self, inner: S) -> Self::Service {
        ChaosLayer::new(inner, ChaosSchedule::clone(&self.schedule))
    }
}

/// Build a chaos layer for `schedule`, ready to wrap a service.
///
/// ```
/// use chaos_kit::{chaos_layer, ChaosSchedule};
/// use tower::Layer;
///
/// let make = chaos_layer(ChaosSchedule::seeded(1));
/// # struct Svc;
/// # impl tower::Service<()> for Svc {
/// #     type Response = (); type Error = chaos_kit::ChaosError;
/// #     type Future = std::future::Ready<Result<(), Self::Error>>;
/// #     fn poll_ready(&mut self, _: &mut std::task::Context<'_>)
/// #         -> std::task::Poll<Result<(), Self::Error>> { std::task::Poll::Ready(Ok(())) }
/// #     fn call(&mut self, (): ()) -> Self::Future { std::future::ready(Ok(())) }
/// # }
/// let service = make.layer(Svc);
/// ```
#[must_use]
pub fn chaos_layer(schedule: ChaosSchedule) -> ChaosMakeLayer {
    ChaosMakeLayer {
        schedule: Arc::new(schedule),
    }
}

/// Build a chaos layer whose injected faults are converted by `map`,
/// for services whose error type **cannot** absorb [`ChaosError`]
/// (`S::Error: From<ChaosError>` does not hold).
///
/// The closure decides what one injected fault becomes, with full access
/// to the wrapped service's types:
///
/// - **Return `Ok(response)`** to turn the fault into a legitimate
///   response. This is the bridge for `axum`: `Router` services are
///   `Infallible`, so an injected error cannot propagate as an error —
///   fold it into the response instead
///   (`Router::layer` accepts the result directly):
///
///   ```
///   use axum::http::StatusCode;
///   use axum::response::{IntoResponse, Response};
///   use chaos_kit::{chaos_layer_map, ChaosError, ChaosSchedule, Fault};
///   # let schedule = ChaosSchedule::seeded(1);
///   let app: axum::Router =
///       axum::Router::new().layer(chaos_layer_map(schedule, |error: ChaosError| {
///           let response: Response = (StatusCode::SERVICE_UNAVAILABLE, format!("chaos: {error}"))
///               .into_response();
///           Ok(response)
///       }));
///   # let _ = app;
///   ```
///
/// - **Return `Err(error)`** to propagate the fault as your own error
///   type — for error enums without a `#[from] ChaosError` variant, or
///   ones you do not own:
///
///   ```
///   use chaos_kit::{chaos_layer_map, ChaosError, ChaosSchedule};
///   use tower::Layer;
///   # let schedule = ChaosSchedule::seeded(1);
///   # #[derive(Debug)]
///   # enum MyError { Chaos(ChaosError) }
///   # let make = chaos_layer_map(schedule, |error| Err::<(), _>(MyError::Chaos(error)));
///   # struct Svc;
///   # impl tower::Service<()> for Svc {
///   #     type Response = (); type Error = MyError;
///   #     type Future = std::future::Ready<Result<(), Self::Error>>;
///   #     fn poll_ready(&mut self, _: &mut std::task::Context<'_>)
///   #         -> std::task::Poll<Result<(), Self::Error>> { std::task::Poll::Ready(Ok(())) }
///   #     fn call(&mut self, (): ()) -> Self::Future { std::future::ready(Ok(())) }
///   # }
///   # let _ = make.layer(Svc);
///   ```
///
/// Scheduling semantics are identical to [`chaos_layer`]: one [`Fault`]
/// per call index, latency sleeps on tokio time, and the shared
/// [`ChaosRecorder`](crate::ChaosRecorder) counts every applied fault.
///
/// # axum caveat — attach via `route_service` for index-exact chaos
///
/// The call counter lives in the layer instance. axum re-materializes
/// layered services around `get(handler)`-style routes **per request**
/// (`BoxedIntoRoute::into_route` re-runs `Layer::layer`), so every request
/// would restart at index 0 — `fault_at(0, …)` would fire on every call
/// and `fault_at(1, …)` never. Attaching through
/// [`Router::route_service`](axum::routing::Router::route_service) (or any
/// service-based route) wraps the service once, so the counter advances
/// across requests and schedules stay index-exact:
///
/// ```
/// use axum::body::Body;
/// use axum::http::{Request, StatusCode};
/// use axum::response::{IntoResponse, Response};
/// use chaos_kit::{chaos_layer_map, ChaosError, ChaosSchedule, Fault};
/// use tower::{Layer, Service};
/// # use std::convert::Infallible;
/// # use std::task::{Context, Poll};
/// # #[derive(Clone)] struct Echo;
/// # impl Service<Request<Body>> for Echo {
/// #     type Response = Response;
/// #     type Error = Infallible;
/// #     type Future = std::future::Ready<Result<Response, Infallible>>;
/// #     fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Infallible>> { Poll::Ready(Ok(())) }
/// #     fn call(&mut self, _: Request<Body>) -> Self::Future {
/// #         std::future::ready(Ok(StatusCode::OK.into_response()))
/// #     }
/// # }
/// let make = chaos_layer_map(
///     ChaosSchedule::seeded(1).fault_at(0, Fault::Error(ChaosError::Timeout)),
///     |error: ChaosError| {
///         let response: Response =
///             (StatusCode::SERVICE_UNAVAILABLE, format!("chaos: {error}")).into_response();
///         Ok(response)
///     },
/// );
/// let app: axum::Router = axum::Router::new().route_service("/", make.layer(Echo));
/// # let _ = app;
/// ```
#[must_use]
pub fn chaos_layer_map<F>(schedule: ChaosSchedule, map: F) -> ChaosMakeMapLayer<F> {
    ChaosMakeMapLayer {
        schedule: Arc::new(schedule),
        map,
    }
}

/// A service wrapped in chaos: applies one [`Fault`] per call index.
///
/// The layer keeps its own call counter, so a fresh layer starts at index
/// 0; clones share the counter (and the schedule recorder). The wrapped
/// service's error type must absorb [`ChaosError`] (`S::Error:
/// From<ChaosError>`), which most error enums do via a `#[from]` variant.
/// When it cannot — axum's `Infallible` services, error types you do not
/// own — use [`chaos_layer_map`] instead, which converts each injected
/// fault through a closure.
///
/// Faults map to call-path effects (see [`Fault`]); everything else is
/// forwarded untouched. The shared recorder counts every applied fault —
/// assert with [`ChaosLayer::recorder`].
#[derive(Debug, Clone)]
pub struct ChaosLayer<S> {
    inner: S,
    schedule: Arc<ChaosSchedule>,
    next_index: Arc<AtomicU64>,
}

impl<S> ChaosLayer<S> {
    /// Wrap `inner` so calls are chaos-gated by `schedule`.
    #[must_use]
    pub fn new(inner: S, schedule: ChaosSchedule) -> Self {
        Self {
            inner,
            schedule: Arc::new(schedule),
            next_index: Arc::new(AtomicU64::new(0)),
        }
    }

    /// The schedule driving this layer.
    #[must_use]
    pub fn schedule(&self) -> &ChaosSchedule {
        &self.schedule
    }

    /// The shared injection recorder — counts applied faults per kind.
    #[must_use]
    pub fn recorder(&self) -> &ChaosRecorder {
        self.schedule.recorder()
    }
}

impl<S, Request> Service<Request> for ChaosLayer<S>
where
    S: Service<Request>,
    S::Error: From<ChaosError>,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = ChaosFuture<S::Future, S::Error>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: Request) -> Self::Future {
        let index = self.next_index.fetch_add(1, Ordering::Relaxed);
        match self.schedule.fault_for(index) {
            Some(Fault::Latency(delay)) => {
                self.schedule.record(FaultKind::Latency);
                ChaosFuture::delayed(Box::pin(tokio::time::sleep(*delay)), self.inner.call(req))
            }
            Some(Fault::Error(error)) => {
                self.schedule.record(FaultKind::Error);
                ChaosFuture::faulted(S::Error::from(error.clone()))
            }
            Some(Fault::Partition { .. }) => {
                // Both directions sever before the inner service is called;
                // direction and duration are model metadata (see `Fault`).
                self.schedule.record(FaultKind::Partition);
                ChaosFuture::faulted(S::Error::from(ChaosError::Unavailable))
            }
            Some(Fault::Throttle { rate }) if self.schedule.drops(index, *rate) => {
                self.schedule.record(FaultKind::Throttle);
                ChaosFuture::faulted(S::Error::from(ChaosError::Unavailable))
            }
            Some(Fault::Cpu(duration)) => {
                self.schedule.record(FaultKind::Cpu);
                burn_cpu(*duration);
                ChaosFuture::passthrough(self.inner.call(req))
            }
            // Clean index, or a Throttle this index survives.
            _ => ChaosFuture::passthrough(self.inner.call(req)),
        }
    }
}

/// Busy-spin one core for `duration` — CPU starvation, test-only.
///
/// This deliberately wastes real cycles on the calling thread; keep test
/// durations tiny and never use it in production middleware.
fn burn_cpu(duration: Duration) {
    if duration.is_zero() {
        return;
    }
    let start = std::time::Instant::now();
    while start.elapsed() < duration {
        std::hint::spin_loop();
    }
}

pin_project! {
    /// The future for one chaos-gated call.
    ///
    /// Either forwards to the wrapped service, sleeps (virtual tokio time)
    /// then forwards, or resolves immediately with the injected error.
    #[derive(Debug)]
    pub struct ChaosFuture<F, E> {
        #[pin]
        inner: ChaosInner<F, E>,
    }
}

pin_project! {
    #[project = ChaosInnerProj]
    #[derive(Debug)]
    enum ChaosInner<F, E> {
        /// Forward directly to the wrapped service.
        Passthrough {
            #[pin]
            future: F,
        },
        /// Sleep, then forward (`Option` re-polls cleanly after the timer).
        Delayed {
            sleep: Pin<Box<Sleep>>,
            #[pin]
            inner: Option<F>,
        },
        /// Resolve immediately with the injected error.
        Faulted {
            error: Option<E>,
        },
    }
}

impl<F, E> ChaosFuture<F, E> {
    fn passthrough(future: F) -> Self {
        Self {
            inner: ChaosInner::Passthrough { future },
        }
    }

    fn delayed(sleep: Pin<Box<Sleep>>, inner: F) -> Self {
        Self {
            inner: ChaosInner::Delayed {
                sleep,
                inner: Some(inner),
            },
        }
    }

    fn faulted(error: E) -> Self {
        Self {
            inner: ChaosInner::Faulted { error: Some(error) },
        }
    }
}

impl<F, T, E> Future for ChaosFuture<F, E>
where
    F: Future<Output = Result<T, E>>,
{
    type Output = Result<T, E>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.project().inner.project() {
            ChaosInnerProj::Passthrough { future } => future.poll(cx),
            ChaosInnerProj::Delayed { sleep, mut inner } => match sleep.as_mut().poll(cx) {
                Poll::Ready(()) => match inner.as_mut().as_pin_mut() {
                    Some(future) => future.poll(cx),
                    None => Poll::Pending,
                },
                Poll::Pending => Poll::Pending,
            },
            // A second poll after readiness is a contract violation; stay
            // pending rather than panic on it.
            ChaosInnerProj::Faulted { error } => match error.take() {
                Some(error) => Poll::Ready(Err(error)),
                None => Poll::Pending,
            },
        }
    }
}

/// A `tower::Layer` that wraps any service in a [`ChaosSchedule`] with a
/// caller-supplied fault mapping (see [`chaos_layer_map`]). Cloning it
/// shares the schedule (and its recorder).
#[derive(Clone)]
pub struct ChaosMakeMapLayer<F> {
    schedule: Arc<ChaosSchedule>,
    map: F,
}

impl<F> std::fmt::Debug for ChaosMakeMapLayer<F> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // The closure is not `Debug`; the schedule carries the identity.
        f.debug_struct("ChaosMakeMapLayer")
            .field("schedule", &self.schedule)
            .finish_non_exhaustive()
    }
}

impl<S, F> Layer<S> for ChaosMakeMapLayer<F>
where
    F: Clone,
{
    type Service = ChaosMapLayer<S, F>;

    fn layer(&self, inner: S) -> Self::Service {
        ChaosMapLayer {
            inner,
            schedule: Arc::clone(&self.schedule),
            next_index: Arc::new(AtomicU64::new(0)),
            map: self.map.clone(),
        }
    }
}

/// A service wrapped in chaos with a caller-supplied fault mapping (see
/// [`chaos_layer_map`]).
///
/// Scheduling, cloning, and recorder semantics match [`ChaosLayer`]; the
/// difference is the error contract: instead of requiring
/// `S::Error: From<ChaosError>`, every injected fault goes through the
/// mapping closure, which may produce a response (the `axum` /
/// `Infallible` bridge) or an `S::Error` of its own choosing.
#[derive(Clone)]
pub struct ChaosMapLayer<S, F> {
    inner: S,
    schedule: Arc<ChaosSchedule>,
    next_index: Arc<AtomicU64>,
    map: F,
}

impl<S, F> ChaosMapLayer<S, F> {
    /// The schedule driving this layer.
    #[must_use]
    pub fn schedule(&self) -> &ChaosSchedule {
        &self.schedule
    }

    /// The shared injection recorder — counts applied faults per kind.
    #[must_use]
    pub fn recorder(&self) -> &ChaosRecorder {
        self.schedule.recorder()
    }
}

impl<S, F> std::fmt::Debug for ChaosMapLayer<S, F>
where
    S: std::fmt::Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // The closure is not `Debug`; the schedule carries the identity.
        f.debug_struct("ChaosMapLayer")
            .field("inner", &self.inner)
            .field("schedule", &self.schedule)
            .finish_non_exhaustive()
    }
}

impl<S, Request, F> Service<Request> for ChaosMapLayer<S, F>
where
    S: Service<Request>,
    F: Fn(ChaosError) -> Result<S::Response, S::Error> + Send + Sync + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = ChaosMapFuture<S::Future, S::Response, S::Error>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: Request) -> Self::Future {
        let index = self.next_index.fetch_add(1, Ordering::Relaxed);
        match self.schedule.fault_for(index) {
            Some(Fault::Latency(delay)) => {
                self.schedule.record(FaultKind::Latency);
                ChaosMapFuture::delayed(Box::pin(tokio::time::sleep(*delay)), self.inner.call(req))
            }
            Some(Fault::Error(error)) => {
                self.schedule.record(FaultKind::Error);
                ChaosMapFuture::faulted((self.map)(error.clone()))
            }
            Some(Fault::Partition { .. }) => {
                // Both directions sever before the inner service is called;
                // direction and duration are model metadata (see `Fault`).
                self.schedule.record(FaultKind::Partition);
                ChaosMapFuture::faulted((self.map)(ChaosError::Unavailable))
            }
            Some(Fault::Throttle { rate }) if self.schedule.drops(index, *rate) => {
                self.schedule.record(FaultKind::Throttle);
                ChaosMapFuture::faulted((self.map)(ChaosError::Unavailable))
            }
            Some(Fault::Cpu(duration)) => {
                self.schedule.record(FaultKind::Cpu);
                burn_cpu(*duration);
                ChaosMapFuture::passthrough(self.inner.call(req))
            }
            // Clean index, or a Throttle this index survives.
            _ => ChaosMapFuture::passthrough(self.inner.call(req)),
        }
    }
}

pin_project! {
    /// The future for one chaos-gated call through [`chaos_layer_map`].
    ///
    /// Either forwards to the wrapped service, sleeps (virtual tokio time)
    /// then forwards, or resolves immediately with the mapped fault
    /// outcome (a response or an error, per the closure).
    #[derive(Debug)]
    pub struct ChaosMapFuture<F, T, E> {
        #[pin]
        inner: ChaosMapInner<F, T, E>,
    }
}

pin_project! {
    #[project = ChaosMapInnerProj]
    #[derive(Debug)]
    enum ChaosMapInner<F, T, E> {
        /// Forward directly to the wrapped service.
        Passthrough {
            #[pin]
            future: F,
        },
        /// Sleep, then forward (`Option` re-polls cleanly after the timer).
        Delayed {
            sleep: Pin<Box<Sleep>>,
            #[pin]
            inner: Option<F>,
        },
        /// Resolve immediately with the mapped fault outcome.
        Faulted {
            outcome: Option<Result<T, E>>,
        },
    }
}

impl<F, T, E> ChaosMapFuture<F, T, E> {
    fn passthrough(future: F) -> Self {
        Self {
            inner: ChaosMapInner::Passthrough { future },
        }
    }

    fn delayed(sleep: Pin<Box<Sleep>>, inner: F) -> Self {
        Self {
            inner: ChaosMapInner::Delayed {
                sleep,
                inner: Some(inner),
            },
        }
    }

    fn faulted(outcome: Result<T, E>) -> Self {
        Self {
            inner: ChaosMapInner::Faulted {
                outcome: Some(outcome),
            },
        }
    }
}

impl<F, T, E> Future for ChaosMapFuture<F, T, E>
where
    F: Future<Output = Result<T, E>>,
{
    type Output = Result<T, E>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.project().inner.project() {
            ChaosMapInnerProj::Passthrough { future } => future.poll(cx),
            ChaosMapInnerProj::Delayed { sleep, mut inner } => match sleep.as_mut().poll(cx) {
                Poll::Ready(()) => match inner.as_mut().as_pin_mut() {
                    Some(future) => future.poll(cx),
                    None => Poll::Pending,
                },
                Poll::Pending => Poll::Pending,
            },
            // A second poll after readiness is a contract violation; stay
            // pending rather than panic on it.
            ChaosMapInnerProj::Faulted { outcome } => match outcome.take() {
                Some(ready) => Poll::Ready(ready),
                None => Poll::Pending,
            },
        }
    }
}
