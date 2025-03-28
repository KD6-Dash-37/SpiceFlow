use crate::http_api::handle::{OrchestratorHandle, SubscriptionAction};
use crate::http_api::mock_ref_data::resolve_subscription;
use axum::{
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::post,
    serve, Json, Router,
};
use serde::Deserialize;
use serde_json::json;
use std::net::SocketAddr;
use tokio::net::TcpListener;
use tracing::{error, info, warn};

#[derive(Debug, Deserialize)]
pub struct SubscriptionRequest {
    pub base_currency: String,
    pub quote_currency: String,
    pub instrument_type: String,
    pub exchange: String,
    pub requested_feed: String,
}

pub async fn start_http_server(handle: OrchestratorHandle) {
    let app = Router::new()
        .route("/subscribe", post(subscribe_handler))
        .route("/unsubscribe", post(unsubscribe_handler))
        .with_state(handle);
    let addr = SocketAddr::from(([127, 0, 0, 1], 3000));
    let listener = TcpListener::bind(addr)
        .await
        .expect("Failed to bind address");
    info!("📡 HTTP server listening on http://{}", addr);
    serve(listener, app).await.expect("Server failed");
}

pub async fn subscribe_handler(
    State(handle): State<OrchestratorHandle>,
    Json(request): Json<SubscriptionRequest>,
) -> Response {
    match resolve_subscription(&request) {
        Some(sub) => {
            let stream_id = sub.stream_id().to_string();
            let action = SubscriptionAction::Subscribe(sub);
            if let Err(e) = handle.send(action).await {
                error!(
                    "❌ Failed to send SubscriptionAction::Subscribe to orchestrator: {:?}",
                    e
                );
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({ "error": "Failed to process request" })),
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
        None => {
            warn!("❌ Invalid subscribe request: {:?}", request);
            (
                StatusCode::BAD_REQUEST,
                Json(json!({
                    "error": "Invalid subscription parameters",
                })),
            )
                .into_response()
        }
    }
}

pub async fn unsubscribe_handler(
    State(handle): State<OrchestratorHandle>,
    Json(request): Json<SubscriptionRequest>,
) -> Response {
    match resolve_subscription(&request) {
        Some(sub) => {
            let stream_id = sub.stream_id().to_string();
            let action = SubscriptionAction::Unsubscribe(sub);
            if let Err(e) = handle.send(action).await {
                error!(
                    "❌ Failed to send SubscriptionAction::Unsubscribe to orchestrator: {:?}",
                    e
                );
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({"error": "Failed to process unsubscribe request"})),
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
        None => {
            warn!("❌ Invalid unsubscribe request: {:?}", request);
            (
                StatusCode::BAD_REQUEST,
                Json(json!({
                    "error": "Invalid unsubscribe parameters",
                })),
            )
                .into_response()
        }
    }
}
