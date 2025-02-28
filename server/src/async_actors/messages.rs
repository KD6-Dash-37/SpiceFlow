use tokio_tungstenite::tungstenite::protocol::Message;
use crate::async_actors::orchestrator::common::RequestedFeed;

use crate::async_actors::subscription::ExchangeSubscription;
use std::fmt;

#[derive(Debug)]
pub enum DummyRequest {
    Subscribe {
        internal_symbol: String,
        exchange: String,
        exchange_symbol: String,
        requested_feed: RequestedFeed,
    },
    Unsubscribe {
        internal_symbol: String,
        exchange: String,
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
    Subscribe(ExchangeSubscription),
    Unsubscribe(ExchangeSubscription),
    Teardown,
}

#[derive(Debug, Clone)]
pub struct ExchangeMessage {
    pub actor_id: String,
    pub message: Message,
}

impl ExchangeMessage {
    pub fn new(actor_id: String, message: Message) -> Self {
        Self { actor_id, message }
    }
}

#[derive(Clone)]
pub enum RouterMessage {
    Heartbeat { actor_id: String },
    ConfirmSubscribe {
        ws_actor_id: String,
        exchange_symbol: String,
        feed_type: RequestedFeed,
    },
    ConfirmUnsubscribe {
        ws_actor_id: String,
        exchange_symbol: String,
        feed_type: RequestedFeed,
    }
}

pub enum RouterCommand {
    Register {
        subscription: ExchangeSubscription,
    },
    Remove {
        subscription: ExchangeSubscription,
    }
}

pub struct MarketData {
    pub topic: String,
    pub data: serde_json::Value,
}