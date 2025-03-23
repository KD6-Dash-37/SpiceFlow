use crate::async_actors::messages::{
    BroadcastActorCommand, BroadcastActorMessage, ProcessedMarketData, ProcessedOrderBookData
};
use flatbuffers::{FlatBufferBuilder, WIPOffset};
use futures_util::lock::Mutex;
use tokio::sync::mpsc;
use tokio::time::{self, Duration};
use tracing::{error, info, warn, Instrument};
use crate::templates::order_book_generated::{OrderBook, OrderBookArgs, PriceLevel, PriceLevelArgs};
use thiserror::Error;
use zmq;
use crate::async_actors::messages::Topic;
pub trait ZmqSocketInterface {
    fn send(&self, data: &[u8], flags: i32) -> Result<(), String>;
}

impl ZmqSocketInterface for zmq::Socket {
    fn send(&self, data: &[u8], flags: i32) -> Result<(), String> {
        self.send(data, flags)
            .map_err(|e| format!("ZeroMQ send error: {:?}", e))
    }
}

const DEFAULT_BUILDER_POOL_SIZE: usize = 10; 
const BROADCAST_ACTOR_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(5);

#[derive(Debug, Error)]
pub enum BroadcastError {
    #[error("Serialisation failed: {0}")]
    Serialisation(String),
    #[error("Failed to send topic: {0}")]
    SendTopic(String),
    #[error("Failed to send data: {0}")]
    SendData(String),
}

pub struct BroadcastActor {
    actor_id: String,
    zmq_sender: mpsc::Sender<(String, Vec<u8>)>,
    market_data_receiver: mpsc::Receiver<ProcessedMarketData>,
    from_orch: mpsc::Receiver<BroadcastActorCommand>,
    to_orch: mpsc::Sender<BroadcastActorMessage>,
    builder_pool: BuilderPool,
}

impl BroadcastActor {
    pub fn new(
        actor_id: String,
        zmq_port: u16,
        market_data_receiver: mpsc::Receiver<ProcessedMarketData>,
        from_orch: mpsc::Receiver<BroadcastActorCommand>,
        to_orch: mpsc::Sender<BroadcastActorMessage>,
    ) -> Self {
        
        let (zmq_sender, mut zmq_receiver) = mpsc::channel::<(String, Vec<u8>)>(32);

        std::thread::Builder::new()
            .name("zmq-publisher".into())
            .spawn(move || {
                let context = zmq::Context::new();
                let socket = context.socket(zmq::PUB).expect("❌ Failed to create ZeroMQ PUB socket");
                let endpoint = format!("tcp://*:{}", zmq_port);
                socket.bind(&endpoint).expect("❌ Failed to bind ZeroMQ PUB socket");
                info!("📡 ZeroMQ PUB socket bound to {}", endpoint);
                while let Some((topic, payload)) = zmq_receiver.blocking_recv() {
                    if let Err(e) = socket.send(topic.as_bytes(), zmq::SNDMORE) {
                        error!("❌ ZMQ topic send failed: {e}");
                        continue;
                    }
                    if let Err(e) = socket.send(payload, 0) {
                        error!("❌ ZMQ payload send failed: {e}");
                    }
                }
                info!("🛑 ZeroMQ publisher thread exiting.");
            })
            .expect("❌ Failed to spawn ZMQ publisher thread");

        Self {
            actor_id,
            zmq_sender,
            market_data_receiver,
            from_orch,
            to_orch,
            builder_pool: BuilderPool::new(DEFAULT_BUILDER_POOL_SIZE),
        }
    }

    pub async fn run(mut self) {
        let span = tracing::info_span!(
            "BroadcastActor",
            actor_id = %self.actor_id,
        );

        async move {
            let mut heartbeat_interval =
                time::interval(BROADCAST_ACTOR_HEARTBEAT_INTERVAL);

            loop {
                tokio::select! {
                    _ = heartbeat_interval.tick() => {
                        if !self.send_heartbeat().await {
                            break;
                        }
                    }

                    Some(data) = self.market_data_receiver.recv() => {
                        match self.serialise_market_data(&data).await {
                            Ok(payload) => {
                                match self.broadcast_data(data.topic(), payload).await {
                                    Ok(_) => {}
                                    Err(e) => {
                                        warn!("Could not broadcast data: {}", e);
                                    }
                                }
                            },
                            Err(e) => {
                                warn!("Could not serialise data: {}", e);
                            }
                        };
                        
                    }

                    Some(command) = self.from_orch.recv() => {
                        match command {
                            BroadcastActorCommand::Shutdown => {
                                info!("Received shutdown command, exiting actor loop");
                                break;
                            }
                        }
                    }
                }
            }
        }
        .instrument(span)
        .await
    }

    async fn send_heartbeat(&mut self) -> bool {
        match self
            .to_orch
            .send(BroadcastActorMessage::Heartbeat {
                actor_id: self.actor_id.clone(),
            })
            .await
        {
            Ok(_) => {
                info!("Sent heartbeat");
                true
            }
            Err(e) => {
                error!("Failed to send heart: {}", e);
                false
            }
        }
    }

    async fn broadcast_data(&self, topic: &str, payload: Vec<u8>) -> Result<(), BroadcastError>{
        match self.zmq_sender
            .send((topic.to_string(), payload))
            .await
        {
            Ok(_) => Ok(()),
            Err(e) => Err(BroadcastError::SendData(
                format!("Failed to send over ZMQ channel: {e}")
            ))
        }

    }

    async fn serialise_market_data(
        &self,
        data: &ProcessedMarketData
    ) -> Result<Vec<u8>, BroadcastError> {
        match data {
            ProcessedMarketData::OrderBook(ob) => {
                self.serialise_order_book_data(ob).await
            }
        }
    }

    async fn serialise_order_book_data(
        &self,
        data: &ProcessedOrderBookData
    ) -> Result<Vec<u8>, BroadcastError> {
        let mut builder = self.builder_pool.acquire_builder().await;
        
        let bids: Vec<WIPOffset<PriceLevel>> = data
            .bids
            .iter()
            .map(|(price, size)|{
                PriceLevel::create(
                    &mut builder,
                    &PriceLevelArgs {
                        price: price.into_inner(),
                        size: *size
                    }
                )
            })
            .collect();
        
        let asks: Vec<WIPOffset<PriceLevel>> = data
            .asks
            .iter()
            .map(|(price, size)| {
                PriceLevel::create(
                    &mut builder,
                    &PriceLevelArgs {
                        price: price.into_inner(),
                        size: *size
                    }
                )
            })
            .collect();
        
        let topic = builder.create_string(&data.stream_id);

        let bids = builder.create_vector(&bids);
        let asks = builder.create_vector(&asks);
        let order_book = OrderBook::create(
            &mut builder,
            &OrderBookArgs {
                topic: Some(topic),
                bids: Some(bids),
                asks: Some(asks),
                exch_timestamp: data.exchange_timestamp,
            },
        );
        builder.finish(order_book, None);
        let result = builder.finished_data().to_vec();
        self.builder_pool.release_builder(builder).await;
        if result.is_empty() {
            return Err(BroadcastError::Serialisation("Empty Flatbuffer".into()));
        }
        Ok(result)
    }
}

pub struct BuilderPool {
    builders: Mutex<Vec<FlatBufferBuilder<'static>>>,
}

impl BuilderPool {
    pub fn new(initial_size: usize) -> Self {
        let builders = (0..initial_size)
            .map(|_| FlatBufferBuilder::new())
            .collect();
        Self {
            builders: Mutex::new(builders),
        }
    }

    pub async fn acquire_builder(&self) -> FlatBufferBuilder<'static> {
        let mut builders = self.builders.lock().await;
        builders.pop().unwrap_or_else(FlatBufferBuilder::new)
    }

    pub async fn release_builder(&self, mut builder: FlatBufferBuilder<'static>) {
        builder.reset();
        let mut builders = self.builders.lock().await;
        builders.push(builder);
    }
}