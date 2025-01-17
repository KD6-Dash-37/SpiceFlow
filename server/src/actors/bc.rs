use crate::actors::ob::ProcessedOrderBookData;
use crate::gen_templates::order_book::{OrderBook, OrderBookArgs, PriceLevel, PriceLevelArgs};

use ::log;
use flatbuffers;
use flatbuffers::{FlatBufferBuilder, WIPOffset};
use futures_util::lock::Mutex;
use thiserror::Error;
use tokio::sync::mpsc;
use zmq;

pub trait ZmqSocketInterface {
    fn send(&self, data: &[u8], flags: i32) -> Result<(), String>;
}

impl ZmqSocketInterface for zmq::Socket {
    fn send(&self, data: &[u8], flags: i32) -> Result<(), String> {
        self.send(data, flags)
            .map_err(|e| format!("ZeroMQ send error: {:?}", e))
    }
}
pub struct BroadcastActor<S: ZmqSocketInterface> {
    receiver: mpsc::Receiver<ProcessedOrderBookData>,
    zmq_socket: S,
    builder_pool: BuilderPool,
}

impl<S: ZmqSocketInterface> BroadcastActor<S> {
    pub fn new(
        receiver: mpsc::Receiver<ProcessedOrderBookData>,
        zmq_socket: S, // Pass in the already initialized zmq_socket
    ) -> Self {
        Self {
            receiver,
            zmq_socket,
            builder_pool: BuilderPool::new(DEFAULT_BUILDER_POOL_SIZE),
        }
    }

    pub async fn run(&mut self) {
        while let Some(data) = self.receiver.recv().await {
            if let Err(err) = self.broadcast_data(data).await {
                eprint!("Failed to process data: {:?}", err);
            }
        }
    }

    async fn broadcast_data(&self, data: ProcessedOrderBookData) -> Result<(), BroadcastError> {
        // Serialise the order book data to FlatBuffer format
        let serialised_data = match self.serialise_order_book_data(&data).await {
            Ok(data) => data,
            Err(e) => {
                log::error!(
                    "Serialisation failed for instrument {}: {:?}",
                    data.topic,
                    e
                );
                return Err(BroadcastError::Serialisation(format!("{:?}", e)));
            }
        };

        // Publish the serialised data on the ZeroMQ socket
        if let Err(e) = self.zmq_socket.send(data.topic.as_bytes(), zmq::SNDMORE) {
            log::error!("Failed to send topic {}: {:?}", data.topic, e);
            return Err(BroadcastError::SendTopic(format!("{:?}", e)));
        }

        if let Err(e) = self.zmq_socket.send(&serialised_data, 0) {
            log::error!(
                "Failed to send data for instrument: {}: {:?}",
                data.topic,
                e
            );
            return Err(BroadcastError::SendData(format!("{:?}", e)));
        }

        Ok(())
    }

    async fn serialise_order_book_data(
        &self,
        data: &ProcessedOrderBookData,
    ) -> Result<Vec<u8>, String> {
        // Acquire builder from the pool
        let mut builder = self.builder_pool.acquire().await;

        // Serialise the bids
        let bids: Vec<WIPOffset<PriceLevel>> = data
            .bids
            .iter()
            .map(|(price, size)| {
                PriceLevel::create(
                    &mut builder,
                    &PriceLevelArgs {
                        price: price.value(),
                        size: *size,
                    },
                )
            })
            .collect();

        // Serialise asks
        let asks: Vec<WIPOffset<PriceLevel>> = data
            .asks
            .iter()
            .map(|(price, size)| {
                PriceLevel::create(
                    &mut builder,
                    &PriceLevelArgs {
                        price: price.value(),
                        size: *size,
                    },
                )
            })
            .collect();

        // Prepare message components
        let instrument_name = builder.create_string(&data.topic);
        let bids_vector = builder.create_vector(&bids);
        let asks_vector = builder.create_vector(&asks);

        // Create the OrderBook FlatBuffer
        let order_book = OrderBook::create(
            &mut builder,
            &OrderBookArgs {
                topic: Some(instrument_name),
                bids: Some(bids_vector),
                asks: Some(asks_vector),
            },
        );

        builder.finish(order_book, None);

        // Extract the serialised data
        let result = builder.finished_data().to_vec();

        // Return the builder to the pool
        self.builder_pool.release(builder).await;

        Ok(result)
    }
}

const DEFAULT_BUILDER_POOL_SIZE: usize = 10; // FIXME move this to config mgt once implemented

pub struct BuilderPool {
    builders: Mutex<Vec<FlatBufferBuilder<'static>>>,
}

impl BuilderPool {
    // Create a new pool
    pub fn new(initial_size: usize) -> Self {
        let builders = (0..initial_size)
            .map(|_| FlatBufferBuilder::new())
            .collect();
        Self {
            builders: Mutex::new(builders),
        }
    }

    // Acquire a builder from the pool
    pub async fn acquire(&self) -> FlatBufferBuilder<'static> {
        let mut builders = self.builders.lock().await;
        builders.pop().unwrap_or_else(FlatBufferBuilder::new)
    }

    // Return a builder to the pool
    pub async fn release(&self, mut builder: FlatBufferBuilder<'static>) {
        builder.reset();
        let mut builders = self.builders.lock().await;
        builders.push(builder);
    }
}

#[derive(Debug, Error)]
pub enum BroadcastError {
    #[error("Serialisation failed: {0}")]
    Serialisation(String),
    #[error("Failed to send topic: {0}")]
    SendTopic(String),
    #[error("Failed to send data: {0}")]
    SendData(String),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::actors::ob::{OrderedF64, ProcessedOrderBookData};
    use crate::actors::stream_config::StreamConfig;
    use flatbuffers::root;
    use std::sync::Arc;

    struct MockZmqSocket {
        sent_data: std::sync::Mutex<Vec<Vec<u8>>>,
    }

    impl MockZmqSocket {
        fn new() -> Self {
            Self {
                sent_data: std::sync::Mutex::new(Vec::new()),
            }
        }
    }

    impl ZmqSocketInterface for MockZmqSocket {
        fn send(&self, data: &[u8], _flags: i32) -> Result<(), String> {
            self.sent_data.lock().unwrap().push(data.to_vec());
            Ok(())
        }
    }

    impl ZmqSocketInterface for Arc<MockZmqSocket> {
        fn send(&self, data: &[u8], flags: i32) -> Result<(), String> {
            self.as_ref().send(data, flags)
        }
    }

    #[tokio::test]
    async fn test_serialize_order_book_data() {
        // Create config
        let config = StreamConfig::current();
        let topic = format!("{}.{}", config.internal_symbol, config.requested_feed);

        // Create sample processed order book data
        let processed_data = ProcessedOrderBookData {
            topic,
            bids: vec![(OrderedF64(30000.0), 1.0), (OrderedF64(29999.0), 2.5)],
            asks: vec![(OrderedF64(30001.0), 1.5), (OrderedF64(30002.0), 0.5)],
        };

        // Create mock socket
        let mock_socket = MockZmqSocket::new();

        // Create mock receiver
        let receiver = tokio::sync::mpsc::channel(10).1;

        // Create BroadcastActor with mocked socket
        let actor = BroadcastActor::new(receiver, mock_socket);

        // Serialize the data
        let serialised_data = actor
            .serialise_order_book_data(&processed_data)
            .await
            .expect("Serialisation should succeed");

        // Verify that serialized data is not empty
        assert!(!serialised_data.is_empty());

        // Deserialize the FlatBuffer data using `root`
        let order_book =
            root::<OrderBook>(&serialised_data).expect("Deserialization should succeed");

        // Verify instrument name
        assert_eq!(order_book.topic().unwrap(), processed_data.topic);

        // Verify bids
        let bids = order_book.bids().unwrap();
        assert_eq!(bids.len(), 2);
        assert_eq!(bids.get(0).price(), 30000.0);
        assert_eq!(bids.get(0).size(), 1.0);
        assert_eq!(bids.get(1).price(), 29999.0);
        assert_eq!(bids.get(1).size(), 2.5);

        // Verify asks
        let asks = order_book.asks().unwrap();
        assert_eq!(asks.len(), 2);
        assert_eq!(asks.get(0).price(), 30001.0);
        assert_eq!(asks.get(0).size(), 1.5);
        assert_eq!(asks.get(1).price(), 30002.0);
        assert_eq!(asks.get(1).size(), 0.5);
    }

    #[tokio::test]
    async fn test_broadcast_actor_run_success() {
        use std::sync::Arc;

        // Create a mock ZeroMQ socket
        let mock_socket = Arc::new(MockZmqSocket::new());

        // Create config
        let config = StreamConfig::current();
        let topic = format!("{}.{}", config.internal_symbol, config.requested_feed);

        // Create a channel and feed a message
        let (sender, receiver) = tokio::sync::mpsc::channel(10);
        let processed_data = ProcessedOrderBookData {
            topic,
            bids: vec![(OrderedF64(30000.0), 1.0)],
            asks: vec![(OrderedF64(30001.0), 1.5)],
        };
        sender.send(processed_data).await.unwrap();

        // Create the BroadcastActor with the cloned Arc
        let mut actor = BroadcastActor::new(receiver, Arc::clone(&mock_socket));

        // Run the actor
        tokio::spawn(async move {
            actor.run().await;
        });

        // Verify data was broadcast (via MockZmqSocket)
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        let sent_data = mock_socket.sent_data.lock().unwrap();
        assert_eq!(sent_data.len(), 2); // Topic and serialized data
    }

    #[tokio::test]
    async fn test_builder_pool_reuse() {
        let pool = BuilderPool::new(1);

        // Acquire a builder and finalize a dummy FlatBuffer object
        let mut builder1 = pool.acquire().await;
        {
            let dummy_data = builder1.create_string("dummy");
            builder1.finish(dummy_data, None);
        }

        // Return the builder to the pool
        pool.release(builder1).await;

        // Acquire the builder again and verify it can be reused
        let mut builder2 = pool.acquire().await;
        {
            let dummy_data = builder2.create_string("new_dummy");
            builder2.finish(dummy_data, None);

            // Verify that the builder successfully produces a new FlatBuffer
            let bytes = builder2.finished_data();
            assert!(!bytes.is_empty());
        }

        // Return the builder to the pool again
        pool.release(builder2).await;
    }

    #[tokio::test]
    async fn test_broadcast_data_send_failure() {
        // Create a mock socket that always fails
        struct FailingMockSocket;
        impl ZmqSocketInterface for FailingMockSocket {
            fn send(&self, _data: &[u8], _flags: i32) -> Result<(), String> {
                Err("Mock failure".to_string())
            }
        }

        let receiver = tokio::sync::mpsc::channel(10).1; // Empty receiver
        let actor = BroadcastActor::new(receiver, FailingMockSocket);

        // Create config
        let config = StreamConfig::current();
        let topic = format!("{}.{}", config.internal_symbol, config.requested_feed);

        // Create valid `ProcessedOrderBookData`
        let valid_data = ProcessedOrderBookData {
            topic,
            bids: vec![(OrderedF64(30000.0), 1.0)],
            asks: vec![(OrderedF64(30001.0), 1.5)],
        };

        // Attempt to broadcast
        let result = actor.broadcast_data(valid_data).await;

        // Verify that the error is correctly propagated
        assert!(matches!(result, Err(BroadcastError::SendTopic(_))));
    }
}
