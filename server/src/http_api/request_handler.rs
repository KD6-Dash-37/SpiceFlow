// server/src/http_api/request_handler.rs

// 🌍 Standard library
use std::net::SocketAddr;
use std::sync::Arc;

// 📦 External crates
use axum::{
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::post,
    serve, Json, Router,
};
use serde_json::json;
use tokio::net::TcpListener;
use tracing::{error, info};

// 🧠 Internal moduless
use crate::domain::ref_data::{ExchangeRefDataProvider, RefDataService};
use crate::domain::subscription::ExchangeSubscription;
use crate::http_api::handle::{OrchestratorHandle, SubscriptionAction};

use super::requests::{RawSubscriptionRequest, SubscriptionRequest};

#[derive(Clone)]
pub struct AppState {
    pub orchestrator_handle: OrchestratorHandle,
    pub ref_data: Arc<RefDataService>,
}

pub async fn start_http_server(
    orchestrator_handle: OrchestratorHandle,
    ref_data: Arc<RefDataService>,
) {
    let state = AppState {
        orchestrator_handle,
        ref_data,
    };
    let app = Router::new()
        .route("/subscribe", post(subscribe_handler))
        .route("/unsubscribe", post(unsubscribe_handler))
        .with_state(state);

    let addr = SocketAddr::from(([127, 0, 0, 1], 3000));
    let listener = TcpListener::bind(addr)
        .await
        .expect("Failed to bind address");
    info!("📡 HTTP server listening on http://{}", addr);
    serve(listener, app).await.expect("Server failed");
}

pub async fn subscribe_handler(
    State(state): State<AppState>,
    Json(raw): Json<RawSubscriptionRequest>,
) -> Response {
    handle_subscription_action(&state, raw, SubscriptionAction::Subscribe).await
}

pub async fn unsubscribe_handler(
    State(state): State<AppState>,
    Json(raw): Json<RawSubscriptionRequest>,
) -> Response {
    handle_subscription_action(&state, raw, SubscriptionAction::Unsubscribe).await
}

async fn handle_subscription_action(
    state: &AppState,
    raw: RawSubscriptionRequest,
    action_builder: impl FnOnce(ExchangeSubscription) -> SubscriptionAction,
) -> Response {
    let request = match SubscriptionRequest::try_from(raw) {
        Ok(r) => r,
        Err(e) => {
            error!("❌ Invalid request: {}", e);
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"error": e.to_string()})),
            )
                .into_response();
        }
    };

    match state.ref_data.resolve_request(&request).await {
        Ok(subscription) => {
            info!("✅ Resolved subscription: {:?}", subscription);
            let stream_id = subscription.stream_id().to_string();
            let action = action_builder(subscription);

            if let Err(e) = state.orchestrator_handle.send(action).await {
                error!("❌ Failed to send {} to orchestrator: {:?}", request, e);
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({
                        "error": "Failed to process request",
                        "request": request.to_string(),
                    })),
                )
                    .into_response();
            }

            (
                StatusCode::OK,
                Json(json!({
                    "status": "ok",
                    "stream_id": stream_id
                })),
            )
                .into_response()
        }

        Err(err) => (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": err.to_string()})),
        )
            .into_response(),
    }
}
