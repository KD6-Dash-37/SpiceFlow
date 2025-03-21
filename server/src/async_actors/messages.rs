use crate::async_actors::orchestrator::common::RequestedFeed;
use crate::async_actors::subscription::ExchangeSubscription;
use std::fmt;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::protocol::Message;
use ordered_float::OrderedFloat;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Exchange {
    Deribit,
}

impl std::str::FromStr for Exchange {
    type Err = &'static str;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "Deribit" => Ok(Exchange::Deribit),
            _ => Err("Unknown or unsupported exchange"),
        }
    }
}

impl std::fmt::Display for Exchange {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl Exchange {
    pub fn as_str(&self) -> &'static str {
        match self {
            Exchange::Deribit => "Deribit",
        }
    }
}

#[derive(Debug)]
pub enum DummyRequest {
    Subscribe {
        internal_symbol: String,
        exchange: Exchange,
        exchange_symbol: String,
        requested_feed: RequestedFeed,
    },
    Unsubscribe {
        internal_symbol: String,
        exchange: Exchange,
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
    Resubscribe(ExchangeSubscription),
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
    Heartbeat {
        actor_id: String,
    },
    ConfirmSubscribe {
        ws_actor_id: String,
        exchange_symbol: String,
        feed_type: RequestedFeed,
    },
    ConfirmUnsubscribe {
        ws_actor_id: String,
        exchange_symbol: String,
        feed_type: RequestedFeed,
    },
}

pub enum RouterCommand {
    Subscribe {
        subscription: ExchangeSubscription,
        raw_market_data_sender: mpsc::Sender<RawMarketData>,
    },
    Remove {
        subscription: ExchangeSubscription,
    },
}

#[derive(Debug)]
pub struct RawMarketData {
    pub stream_id: String,
    pub data: serde_json::Value,
}

pub enum OrderBookMessage {
    Heartbeat { actor_id: String },
    Shutdown { actor_id: String },
    Resubscribe {subscription: ExchangeSubscription},
}

pub enum OrderBookCommand {
    Shutdown,
}

#[derive(Debug)]
pub struct ProcessedOrderBookData {
    pub stream_id: String,
    pub exchange_timestamp: u64,
    pub bids: Vec<(OrderedFloat<f64>, f64)>,
    pub asks: Vec<(OrderedFloat<f64>, f64)>, 
}