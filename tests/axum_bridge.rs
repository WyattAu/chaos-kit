#![allow(clippy::unwrap_used, clippy::expect_used)]
//! The axum `Infallible` bridge: `ChaosLayer` requires
//! `S::Error: From<ChaosError>`, which can never hold for axum — `Router`
//! services are `Infallible`. Hosts bridge with
//! [`chaos_layer_map`](chaos_kit::chaos_layer_map), folding each injected
//! fault into a real response, so the chaos-gated router still satisfies
//! axum's `Into<Infallible>` service bounds.
//!
//! The routes here are service-based (`route_service`) so the layer wraps
//! once and the call counter advances across requests — see the
//! `chaos_layer_map` docs for the `get(handler)` caveat. Tests drive the
//! documented fixture flow end-to-end: a scripted fault schedule, an axum
//! `Router`, and `tower::ServiceExt::oneshot` — no network.

use std::convert::Infallible;
use std::future::ready;
use std::task::{Context, Poll};
use std::time::Duration;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Router;
use chaos_kit::{chaos_layer_map, ChaosError, ChaosSchedule, Direction, Fault, FaultKind};
use tower::{Layer, Service, ServiceExt};

/// Minimal handler service: always answers `200 "ok"`.
#[derive(Clone)]
struct Echo;

impl Service<Request<Body>> for Echo {
    type Response = Response;
    type Error = Infallible;
    type Future = std::future::Ready<Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, _req: Request<Body>) -> Self::Future {
        ready(Ok((StatusCode::OK, "ok").into_response()))
    }
}

/// Build the chaos-gated router: index 0 faults per `schedule`, every
/// other call reaches the handler untouched. Injected faults render as
/// 503s carrying the chaos error — the host-side half of the bridge.
fn chaos_router(schedule: ChaosSchedule) -> Router {
    let make = chaos_layer_map(schedule, |error: ChaosError| {
        let response: Response =
            (StatusCode::SERVICE_UNAVAILABLE, format!("chaos: {error}")).into_response();
        Ok::<_, Infallible>(response)
    });
    Router::new().route_service("/", make.layer(Echo))
}

#[tokio::test]
async fn scripted_fault_becomes_a_response_through_the_router() {
    let schedule = ChaosSchedule::seeded(7).fault_at(0, Fault::Error(ChaosError::Timeout));
    let app = chaos_router(schedule);

    // Call index 0: the scripted fault fires — and surfaces as a 503
    // response, not an error, because the bridge folded it in.
    let response = app
        .clone()
        .oneshot(Request::get("/").body(Body::empty()).unwrap())
        .await
        .expect("infallible service cannot error");
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    let body = axum::body::to_bytes(response.into_body(), 1 << 16)
        .await
        .unwrap();
    assert!(
        String::from_utf8_lossy(&body).contains("chaos:"),
        "the faulted response should carry the chaos reason, got {body:?}"
    );

    // Call index 1: clean — the handler answers untouched.
    let response = app
        .oneshot(Request::get("/").body(Body::empty()).unwrap())
        .await
        .expect("infallible service cannot error");
    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), 1 << 16)
        .await
        .unwrap();
    assert_eq!(&body[..], b"ok");
}

#[tokio::test]
async fn partition_fault_bridges_to_unavailable_response() {
    // A partition severs before the handler runs; through the bridge it
    // still produces a well-formed 503 rather than an error.
    let schedule = ChaosSchedule::seeded(3).fault_at(
        0,
        Fault::Partition {
            direction: Direction::Ingress,
            for_duration: Duration::from_secs(1),
        },
    );
    let app = chaos_router(schedule);

    let response = app
        .oneshot(Request::get("/").body(Body::empty()).unwrap())
        .await
        .expect("infallible service cannot error");
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
}

#[tokio::test]
async fn recorder_counts_faults_applied_through_the_bridge() {
    // The shared recorder works through the map bridge exactly as it does
    // for `chaos_layer` — post-scenario assertions stay available.
    let schedule = ChaosSchedule::seeded(7).fault_at(0, Fault::Error(ChaosError::Timeout));
    let app = chaos_router(schedule.clone());

    let _ = app
        .oneshot(Request::get("/").body(Body::empty()).unwrap())
        .await
        .unwrap();

    assert_eq!(
        schedule.recorder().injected(FaultKind::Error),
        1,
        "the bridge must record applied faults"
    );
}
