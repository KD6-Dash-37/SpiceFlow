use crate::async_actors::messages::{
    BroadcastActorCommand, Exchange, ExchangeMessage, OrderBookCommand, ProcessedMarketData,
    RawMarketData, RouterCommand, WebSocketCommand,
};
use crate::async_actors::subscription::ExchangeSubscription;
use std::collections::HashSet;
use std::time::Instant;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

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
        join_handle: JoinHandle<()>,
    ) -> Self {
        Self {
            actor_id,
            exchange,
            router_sender,
            router_command_sender,
            subscribed_streams: HashSet::new(),
            last_heartbeat: None,
            join_handle,
        }
    }

    pub fn is_ready(&self) -> bool {
        self.last_heartbeat.is_some()
    }
}

pub struct OrderBookMetadata {
    pub actor_id: String,
    pub subscription: ExchangeSubscription,
    pub last_heartbeat: Option<Instant>,
    pub orderbook_command_sender: mpsc::Sender<OrderBookCommand>,
    pub raw_market_data_sender: mpsc::Sender<RawMarketData>,
    pub join_handle: JoinHandle<()>,
}

impl OrderBookMetadata {
    pub fn new(
        actor_id: String,
        subscription: ExchangeSubscription,
        orderbook_command_sender: mpsc::Sender<OrderBookCommand>,
        raw_market_data_sender: mpsc::Sender<RawMarketData>,
        join_handle: JoinHandle<()>,
    ) -> Self {
        Self {
            actor_id,
            subscription,
            orderbook_command_sender,
            last_heartbeat: None,
            raw_market_data_sender,
            join_handle,
        }
    }

    pub fn is_ready(&self) -> bool {
        self.last_heartbeat.is_some()
    }
}

pub struct BroadcastActorMetadata {
    pub actor_id: String,
    pub last_heartbeat: Option<Instant>,
    pub market_data_sender: mpsc::Sender<ProcessedMarketData>,
    pub command_sender: mpsc::Sender<BroadcastActorCommand>,
    pub join_handle: JoinHandle<()>,
}

impl BroadcastActorMetadata {
    pub fn new(
        actor_id: String,
        market_data_sender: mpsc::Sender<ProcessedMarketData>,
        command_sender: mpsc::Sender<BroadcastActorCommand>,
        join_handle: JoinHandle<()>,
    ) -> Self {
        Self {
            actor_id,
            last_heartbeat: None,
            market_data_sender,
            command_sender,
            join_handle,
        }
    }

    pub fn is_ready(&self) -> bool {
        self.last_heartbeat.is_some()
    }
}
