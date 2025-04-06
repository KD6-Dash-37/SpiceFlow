// server/src/async_actors/messages.rs

// 🌍 Standard library
use std::fmt;

// 📦 External Crates
use ordered_float::OrderedFloat;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::protocol::Message;

// 🧠 Internal Crates / Modules
use crate::domain::ExchangeSubscription;
use crate::model::RequestedFeed;

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
    Resubscribe { subscription: ExchangeSubscription },
}

pub enum OrderBookCommand {
    Shutdown,
}

pub trait Topic {
    fn topic(&self) -> &str;
}

#[derive(Debug)]
pub enum ProcessedMarketData {
    OrderBook(ProcessedOrderBookData),
}

impl Topic for ProcessedMarketData {
    fn topic(&self) -> &str {
        match self {
            ProcessedMarketData::OrderBook(ob) => ob.topic(),
        }
    }
}

#[derive(Debug)]
pub struct ProcessedOrderBookData {
    pub stream_id: String,
    pub exchange_timestamp: u64,
    pub bids: Vec<(OrderedFloat<f64>, f64)>,
    pub asks: Vec<(OrderedFloat<f64>, f64)>,
}

impl Topic for ProcessedOrderBookData {
    fn topic(&self) -> &str {
        &self.stream_id
    }
}

pub enum BroadcastActorMessage {
    Heartbeat { actor_id: String },
}

pub enum BroadcastActorCommand {
    Shutdown,
}
