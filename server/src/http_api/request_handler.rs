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
use tokio::sync::mpsc;
use tracing::{error, info};

// 🧠 Internal moduless
use crate::domain::ref_data::{ExchangeRefDataProvider, RefDataService};
use crate::domain::subscription::ExchangeSubscription;
use crate::async_actors::orchestrator::OrchestratorError;

use super::requests::{RawSubscriptionRequest, SubscriptionRequest};


#[derive(Debug, Clone)]
pub enum SubscriptionAction {
    Subscribe(ExchangeSubscription),
    Unsubscribe(ExchangeSubscription),
}

#[derive(Clone)]
pub struct OrchHandle {
    sender: mpsc::Sender<SubscriptionAction>,
}

impl OrchHandle {
    #[must_use]
    pub const fn new(sender: mpsc::Sender<SubscriptionAction>) -> Self {
        Self { sender }
    }

    /// Sends a [`SubscriptionAction`] to the orchestrator for processing.
    ///
    /// # Errors
    ///
    /// Returns [`OrchestratorError::ChannelClosed`] if the underlying channel is closed
    /// and the message cannot be delivered.
    pub async fn send(&self, sub: SubscriptionAction) -> Result<(), OrchestratorError> {
        self.sender
            .send(sub)
            .await
            .map_err(|_| OrchestratorError::ChannelClosed)
    }
}



#[derive(Clone)]
pub struct AppState {
    pub orchestrator_handle: OrchHandle,
    pub ref_data: Arc<RefDataService>,
}

pub async fn start_http_server(
    orchestrator_handle: OrchHandle,
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
    let listener = match TcpListener::bind(addr).await {
        Ok(l) => l,
        Err(e) => {
            error!("❌ Failed to bind address {}: {}", addr, e);
            return;
        }
    };    
    info!("📡 HTTP server listening on http://{}", addr);
    match serve(listener, app).await {
        Ok(()) => (),
        Err(e) => {
            error!("Server failed: {e}");
        }
    }
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
    action_builder: impl FnOnce(ExchangeSubscription) -> SubscriptionAction + Send + 'static,
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
            let stream_id = subscription.stream_id.clone();
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
