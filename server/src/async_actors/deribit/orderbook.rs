// server/src/async_actors/deribit/orderbook.rs

// 🌍 Standard library
use std::collections::BTreeMap;
use std::fmt;

// 📦 External Crates
use ordered_float::OrderedFloat;
use serde;
use serde::Deserialize;
use serde_tuple::Deserialize_tuple;
use thiserror::Error;
use tokio::sync::mpsc;
use tokio::time::{self, Duration};
use tracing::{error, info, warn, Instrument};

// 🧠 Internal Crates / Modules
use crate::async_actors::messages::{
    OrderBookCommand, OrderBookMessage, ProcessedMarketData, ProcessedOrderBookData, RawMarketData,
};
use crate::domain::ExchangeSubscription;


const ORDERBOOK_HEARTBEAT_INTERVAL: u64 = 5;

#[derive(Debug, Error)]
pub enum OrderBookActorError {
    #[error("❌ parsing error for ")]
    JsonParseError,
    #[error("Validation failed: {0}")]
    ValidationError(String),
    #[error("{0}")]
    MissingField(String),
    #[error("Received orderbook change while waiting for fresh snapshot")]
    UnwantedOrderBookChange,
    #[error("Message send failure")]
    OrchestratorTimeout,
    #[error("Message send failure")]
    BroadcastActorTimeout,
}

type OrderBookResult<T> = Result<T, OrderBookActorError>;

pub enum ProcessMessageResult {
    Ok,
    ErrRequiresResub(OrderBookActorError),
    ErrNoResub(OrderBookActorError),
}

#[derive(Debug, PartialEq, Eq)]
enum OrderBookSide {
    Bids,
    Asks,
}

impl OrderBookSide {
    fn get_map<'a>(
        &self,
        actor: &'a mut DeribitOrderBookActor,
    ) -> &'a mut BTreeMap<OrderedFloat<f64>, f64> {
        match self {
            OrderBookSide::Bids => &mut actor.bids,
            &OrderBookSide::Asks => &mut actor.asks,
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum OrderBookMessageType {
    Snapshot,
    Change,
}

#[derive(Copy, Clone, Debug, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum OrderBookUpdate {
    New,
    Change,
    Delete,
}

impl fmt::Display for OrderBookUpdate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let variant = match self {
            OrderBookUpdate::New => "new",
            OrderBookUpdate::Change => "change",
            OrderBookUpdate::Delete => "delete",
        };
        write!(f, "{}", variant)
    }
}

#[derive(Debug, Deserialize_tuple)]
pub struct RawOrderBookEntry {
    pub update_type: OrderBookUpdate,
    pub price: f64,
    pub quantity: f64,
}

#[derive(Debug, Deserialize)]
pub struct RawOrderBookData {
    #[serde(rename = "type")]
    pub msg_type: OrderBookMessageType,
    pub timestamp: u64,
    pub instrument_name: String,
    pub change_id: u64,
    pub bids: Vec<RawOrderBookEntry>,
    pub asks: Vec<RawOrderBookEntry>,
    pub prev_change_id: Option<u64>,
}

pub struct DeribitOrderBookActor {
    actor_id: String,
    subscription: ExchangeSubscription,
    to_orch: mpsc::Sender<OrderBookMessage>,
    from_orch: mpsc::Receiver<OrderBookCommand>,
    raw_market_data: mpsc::Receiver<RawMarketData>,
    market_data_sender: mpsc::Sender<ProcessedMarketData>,
    waiting_for_snapshot: bool,
    current_change_id: Option<u64>,
    exchange_timestamp: Option<u64>,
    bids: BTreeMap<OrderedFloat<f64>, f64>,
    asks: BTreeMap<OrderedFloat<f64>, f64>,
}

impl DeribitOrderBookActor {
    pub fn new(
        actor_id: String,
        subscription: ExchangeSubscription,
        to_orch: mpsc::Sender<OrderBookMessage>,
        from_orch: mpsc::Receiver<OrderBookCommand>,
        raw_market_data: mpsc::Receiver<RawMarketData>,
        market_data_sender: mpsc::Sender<ProcessedMarketData>,
    ) -> Self {
        Self {
            actor_id,
            subscription,
            to_orch,
            from_orch,
            raw_market_data,
            market_data_sender,
            waiting_for_snapshot: true,
            current_change_id: None,
            exchange_timestamp: None,
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
        }
    }

    pub async fn run(mut self) {
        let span: tracing::Span = tracing::info_span!(
            "DeribitOrderBookActor",
            actor_id = %self.actor_id,
            stream_id = %self.subscription.stream_id(),
        );

        async move {
            let mut heartbeat_interval =
                time::interval(Duration::from_secs(ORDERBOOK_HEARTBEAT_INTERVAL));

            loop {
                tokio::select! {

                    _ = heartbeat_interval.tick() => {
                        if !self.send_hearbeat().await {
                            break;
                        }
                    }
                    Some(command) = self.from_orch.recv() => {
                        match command {
                            OrderBookCommand::Shutdown => {
                                info!("Received shutdown command, exiting actor loop");
                                break;
                            }
                        }
                    }

                    Some(raw) = self.raw_market_data.recv() => {
                        match self.process_market_data(raw).await {
                            ProcessMessageResult::Ok => {
                                if let Err(e) = self.send_processed_data().await {
                                    error!("{}", e)
                                }
                            },
                            ProcessMessageResult::ErrRequiresResub(e) => {
                                error!("{:?}", e);
                                if let Err(e) = self.request_resubscribe().await {
                                    error!("{:?}", e);
                                }
                            },
                            ProcessMessageResult::ErrNoResub(e) => {
                                warn!("{:?}", e);
                            }
                        }
                    }
                }
            }
            info!("Shutting down")
        }
        .instrument(span)
        .await
    }

    async fn send_hearbeat(&mut self) -> bool {
        match self
            .to_orch
            .send(OrderBookMessage::Heartbeat {
                actor_id: self.actor_id.clone(),
            })
            .await
        {
            Ok(_) => {
                info!("Sent hearbeat");
                true
            }
            Err(e) => {
                error!("Failed to send heartbeat to Orchestrator: {e}");
                false
            }
        }
    }

    async fn process_market_data(&mut self, raw: RawMarketData) -> ProcessMessageResult {
        let parsed: RawOrderBookData = match serde_json::from_value(raw.data) {
            Ok(data) => data,
            Err(e) => {
                error!("Failed to deserialise RawMarketData: {}", e);
                return ProcessMessageResult::ErrRequiresResub(OrderBookActorError::JsonParseError);
            }
        };
        match parsed.msg_type {
            OrderBookMessageType::Snapshot => self.process_orderbook_snapshot(parsed),
            OrderBookMessageType::Change => self.process_orderbook_change(parsed),
        }
    }

    fn process_orderbook_snapshot(&mut self, data: RawOrderBookData) -> ProcessMessageResult {
        for side in &[OrderBookSide::Bids, OrderBookSide::Asks] {
            let map = side.get_map(self);
            map.clear();
            let updates = match side {
                OrderBookSide::Bids => &data.bids,
                OrderBookSide::Asks => &data.asks,
            };

            for entry in updates {
                match entry.update_type {
                    OrderBookUpdate::New => {
                        let price = OrderedFloat(entry.price);
                        map.insert(price, entry.quantity);
                    }
                    other => {
                        error!("Unexpected update type in snapshot: {:?}", other);
                        return ProcessMessageResult::ErrRequiresResub(
                            OrderBookActorError::ValidationError(format!(
                                "Unexpected update type in snapshot: {:?}",
                                other
                            )),
                        );
                    }
                }
            }
        }
        self.current_change_id = Some(data.change_id);
        self.waiting_for_snapshot = false;
        ProcessMessageResult::Ok
    }

    fn process_orderbook_change(&mut self, data: RawOrderBookData) -> ProcessMessageResult {
        if self.waiting_for_snapshot {
            return ProcessMessageResult::ErrNoResub(OrderBookActorError::UnwantedOrderBookChange);
        }

        if let Err(err) = self.check_prev_change_id(&data) {
            error!(
                "Could not validate prev_change_id on orderbook update: {:?}",
                err
            );
            return ProcessMessageResult::ErrRequiresResub(err);
        }
        for side in &[OrderBookSide::Bids, OrderBookSide::Asks] {
            let map = side.get_map(self);
            let updates = match side {
                OrderBookSide::Bids => &data.bids,
                OrderBookSide::Asks => &data.asks,
            };
            for entry in updates {
                let price = OrderedFloat(entry.price);
                match entry.update_type {
                    OrderBookUpdate::New | OrderBookUpdate::Change => {
                        map.insert(price, entry.quantity);
                    }
                    OrderBookUpdate::Delete => {
                        map.remove(&price);
                    }
                }
            }
        }
        self.current_change_id = Some(data.change_id);
        ProcessMessageResult::Ok
    }

    fn check_prev_change_id(&self, data: &RawOrderBookData) -> OrderBookResult<()> {
        if let Some(prev_change_id) = data.prev_change_id {
            if Some(prev_change_id) != self.current_change_id {
                error!(
                    "prev_change_id mismatch. Expected: {:?}, Received: {}",
                    self.current_change_id, prev_change_id
                );
                return Err(OrderBookActorError::ValidationError(format!(
                    "prev_change_id mismatch. Expected: {:?}, Received: {}",
                    self.current_change_id, prev_change_id
                )));
            }
        } else {
            error!("Missing prev_change_id field in data");
            return Err(OrderBookActorError::MissingField(
                "data.prev_change_id".to_string(),
            ));
        }
        Ok(())
    }

    async fn request_resubscribe(&mut self) -> Result<(), OrderBookActorError> {
        self.to_orch
            .send(OrderBookMessage::Resubscribe {
                subscription: self.subscription.clone(),
            })
            .await
            .map_err(|_| OrderBookActorError::OrchestratorTimeout)?;
        self.waiting_for_snapshot = true;
        Ok(())
    }

    async fn send_processed_data(&mut self) -> Result<(), OrderBookActorError> {
        let bids: Vec<(OrderedFloat<f64>, f64)> = self
            .bids
            .iter()
            .rev()
            .map(|(price, quantity)| (*price, *quantity))
            .collect();

        let asks: Vec<(OrderedFloat<f64>, f64)> = self
            .asks
            .iter()
            .map(|(price, quantity)| (*price, *quantity))
            .collect();

        let data = ProcessedMarketData::OrderBook(ProcessedOrderBookData {
            stream_id: self.subscription.stream_id().to_string(),
            exchange_timestamp: self.exchange_timestamp.unwrap_or(0),
            bids,
            asks,
        });

        self.market_data_sender
            .send(data)
            .await
            .map_err(|_| OrderBookActorError::BroadcastActorTimeout)
    }
}
