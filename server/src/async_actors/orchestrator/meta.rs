use crate::async_actors::messages::{ExchangeMessage,RouterCommand,WebSocketCommand};
use std::collections::HashSet;
use crate::async_actors::subscription::ExchangeSubscription;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use std::time::Instant;

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

impl Exchange {
    pub fn as_str(&self) -> &'static str {
        match self {
            Exchange::Deribit => "Deribit",
        }
    }
}

pub struct WebSocketMetadata {
    pub actor_id: String,
    pub command_sender: mpsc::Sender<WebSocketCommand>,
    pub router_actor_id: String,
    pub requested_streams: HashSet<ExchangeSubscription>,
    pub subscribed_streams: HashSet<ExchangeSubscription>,
    pub last_heartbeat: Option<Instant>,
    pub join_handle: JoinHandle<()>,
}

impl WebSocketMetadata {
    pub fn new(
        actor_id: String,
        command_sender: mpsc::Sender<WebSocketCommand>,
        router_actor_id: String,
        requested_streams: HashSet<ExchangeSubscription>,
        join_handle: JoinHandle<()>,
    ) -> Self {
        let subscribed_streams = HashSet::new();
        Self {
            actor_id,
            command_sender,
            router_actor_id,
            requested_streams,
            subscribed_streams,
            last_heartbeat: None,
            join_handle,
        }
    }

    pub fn is_ready(&self) -> bool {
        self.last_heartbeat.is_some()
    }
}


pub struct RouterMetadata {
    pub actor_id: String,
    pub exchange: Exchange,
    pub router_sender: mpsc::Sender<ExchangeMessage>,
    pub router_command_sender: mpsc::Sender<RouterCommand>,
    pub subscribed_streams: HashSet<ExchangeSubscription>,
    pub last_heartbeat: Option<Instant>,
    pub join_handle: JoinHandle<()>,
}

impl RouterMetadata {
    pub fn new(
        actor_id: String,
        exchange: Exchange,
        router_sender: mpsc::Sender<ExchangeMessage>,
        router_command_sender: mpsc::Sender<RouterCommand>,
        join_handle: JoinHandle<()>
    ) -> Self {
        Self {
            actor_id,
            exchange,
            router_sender,
            router_command_sender,
            subscribed_streams: HashSet::new(),
            last_heartbeat: None,
            join_handle
        }
    }

    pub fn is_ready(&self) -> bool {
        self.last_heartbeat.is_some()
    }
}