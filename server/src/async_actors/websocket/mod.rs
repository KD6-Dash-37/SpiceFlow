// server/src/async_actors/websocket/mod.rs

// 🌍 Standard library
use std::fmt;

// 📦 External Crates
use thiserror::Error;
mod deribit;
mod binance;
pub use deribit::{DeribitWebSocketActor, SubscriptionManagementAction};
pub use binance::BinanceWebSocketActor;

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum WebSocketActorError {
    #[error("Timed out sending close frame")]
    Timeout,
    #[error("Connection error: {0}")]
    Connection(String),
    #[error("WebSocket send error: {0}")]
    Send(String),
    #[error("WebSocket write stream not initialized")]
    WriteNotInitialised,
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum WebSocketMessage {
    Heartbeat { actor_id: String },
    Disconnected { actor_id: String },
    Shutdown { actor_id: String },
    Error {
        actor_id: String,
        error: WebSocketActorError,
    }
}

impl fmt::Display for WebSocketMessage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            WebSocketMessage::Heartbeat { actor_id } => write!(f, "Heartbeat from {}", actor_id),
            WebSocketMessage::Disconnected { actor_id } => {
                write!(f, "Disconnected from {}", actor_id)
            }
            WebSocketMessage::Shutdown { actor_id } => {
                write!(f, "Shutdown signal from {}", actor_id)
            }
            WebSocketMessage::Error { actor_id, error } => {
                write!(f, "Error on WebSocket from {} - {}", actor_id, error)
            }
        }
    }
}

// WebSocketActor trait candidates
// - send_heartbeat 
// - create/init actor
// - connect_and_split
