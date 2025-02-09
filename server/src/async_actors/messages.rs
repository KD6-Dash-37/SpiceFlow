use tokio_tungstenite::tungstenite::protocol::Message;

use crate::async_actors::common::{RequestedFeed, Subscription};
use std::fmt;

#[derive(Debug)]
pub enum DummyRequest {
    Subscribe {
        internal_symbol: String,
        exchange_symbol: String,
        requested_feed: RequestedFeed,
    },
    Unsubscribe {
        internal_symbol: String,
        exchange_symbol: String,
        requested_feed: RequestedFeed,
    },
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum WebSocketMessage {
    Heartbeat { actor_id: String },
    Disconnected { actor_id: String },
    Shutdown { actor_id: String },
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
        }
    }
}

#[derive(Debug)]
pub enum WebSocketCommand {
    Subscribe(Subscription),
    Unsubscribe(Subscription),
    Teardown,
}

#[derive(Debug)]
pub struct ExchangeMessage {
    actor_id: String,
    message: Message,
}

impl ExchangeMessage {
    pub fn new(actor_id: String, message: Message) -> Self {
        Self { actor_id, message }
    }
}

#[derive(Clone)]
pub enum RouterMessage {
    Heartbeat { actor_id: String },
}
