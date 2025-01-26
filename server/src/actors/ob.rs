use super::router::ParsedMessage;
use super::stream_config::StreamConfig;
use super::ws::WebSocketCommand;
use log;
use std::collections::BTreeMap;
use std::fmt;
use thiserror::Error;
use tokio::sync::mpsc;

#[derive(Debug, Error)]
pub enum OrderBookActorError {
    #[error("Failed to parse JSON: {0}")]
    JsonParseError(#[from] serde_json::Error),
    #[error("Missing or invalid field: {0}")]
    MissingField(String),
    #[error("Validation failed: {0}")]
    ValidationError(String),
    #[error("BroadcastError: {0}")]
    BroadcastError(String),
    #[error("Websocket command failed: {0}")]
    WebSocketCommandError(String),
    #[error("Generic error: {0}")]
    #[allow(dead_code)]
    Generic(String),
}

type OrderBookResult<T> = Result<T, OrderBookActorError>;

pub enum ProcessMessageResult {
    Ok,
    #[allow(dead_code)]
    ErrRequiresResub(OrderBookActorError),
    #[allow(dead_code)]
    ErrNoResub(OrderBookActorError),
}

#[derive(Debug, PartialEq, Eq)]
enum OrderBookMessageType {
    Snapshot,
    Change,
}

#[derive(Debug, PartialEq, Eq)]
enum OrderBookSide {
    Bids,
    Asks,
}

impl OrderBookSide {
    fn as_str(&self) -> &'static str {
        match self {
            OrderBookSide::Bids => "bids",
            OrderBookSide::Asks => "asks",
        }
    }
    fn get_map<'a>(&self, actor: &'a mut OrderBookActor) -> &'a mut BTreeMap<OrderedF64, f64> {
        match self {
            OrderBookSide::Bids => &mut actor.bids,
            &OrderBookSide::Asks => &mut actor.asks,
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
enum OrderBookUpdate {
    New,
    Change,
    Delete,
}

impl OrderBookUpdate {
    fn from_str(update_type: &str) -> Option<Self> {
        match update_type {
            "new" => Some(OrderBookUpdate::New),
            "change" => Some(OrderBookUpdate::Change),
            "delete" => Some(OrderBookUpdate::Delete),
            _ => None,
        }
    }
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

#[derive(Debug)]
pub struct ProcessedOrderBookData {
    #[allow(dead_code)]
    pub topic: String,
    #[allow(dead_code)]
    pub bids: Vec<(OrderedF64, f64)>,
    #[allow(dead_code)]
    pub asks: Vec<(OrderedF64, f64)>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct OrderedF64(pub f64);

impl Eq for OrderedF64 {}

impl OrderedF64 {
    #[allow(dead_code)]
    pub fn new(value: f64) -> Self {
        OrderedF64(value)
    }
    #[allow(dead_code)]
    pub fn value(&self) -> f64 {
        self.0
    }
}

impl PartialOrd for OrderedF64 {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for OrderedF64 {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Use `partial_cmp` and unwrap safely, since `OrderedF64` must not contain NaN.
        self.0
            .partial_cmp(&other.0)
            .expect("OrderedF64 cannot contain NaN values")
    }
}

pub struct OrderBookActor {
    name: String,
    #[allow(dead_code)]
    config: StreamConfig,
    tick_size: f64,
    bids: BTreeMap<OrderedF64, f64>,
    asks: BTreeMap<OrderedF64, f64>,
    change_id: Option<u64>,
    parsed_data_receiver: mpsc::Receiver<ParsedMessage>,
    broadcast: mpsc::Sender<ProcessedOrderBookData>,
    ws_command_sender: mpsc::Sender<WebSocketCommand>,
}

impl OrderBookActor {
    pub fn new(
        name: &str,
        config: &StreamConfig,
        parsed_data_receiver: mpsc::Receiver<ParsedMessage>,
        broadcast: mpsc::Sender<ProcessedOrderBookData>,
        ws_command_sender: mpsc::Sender<WebSocketCommand>,
    ) -> Self {
        Self {
            name: name.to_string(),
            config: config.clone(),
            tick_size: 0.5,
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
            change_id: None,
            parsed_data_receiver,
            broadcast,
            ws_command_sender,
        }
    }

    // Process incoming messages
    pub async fn run(&mut self) {
        while let Some(message) = self.parsed_data_receiver.recv().await {
            match message {
                ParsedMessage::OrderBook { topic, data } => {
                    if let Err(err) = async {
                        self.process_message(topic, data).await;
                        Ok::<(), OrderBookActorError>(())
                    }
                    .await
                    {
                        log::error!("{}: Error in run loop: {:?}", self.name, err);
                    }
                }
                _ => {
                    log::warn!(
                        "{}: Ignoring non-OrderBook message: {:?}",
                        self.name,
                        std::any::type_name::<ParsedMessage>()
                    )
                }
            }
        }
    }

    // For dev/debug purposes to check price vs. exchange UI, see fn run
    #[allow(dead_code)]
    pub fn best_bid(&self) -> Option<(f64, f64)> {
        self.bids
            .last_key_value()
            .map(|(&OrderedF64(price), &size)| (price, size))
    }

    // For dev/debug purposes to check price vs. exchange UI, see fn run
    #[allow(dead_code)]
    pub fn best_ask(&self) -> Option<(f64, f64)> {
        self.asks
            .first_key_value()
            .map(|(&OrderedF64(price), &size)| (price, size))
    }

    async fn normalise_price(&self, price: f64) -> OrderBookResult<OrderedF64> {
        if price.is_nan() {
            log::warn!("{}: Price is NaN, cannot normalise", self.name);
            return Err(OrderBookActorError::ValidationError(
                "Price is NaN".to_string(),
            ));
        } else if price.is_infinite() {
            log::warn!("{}: Price is infinite, cannot normalise", self.name);
            return Err(OrderBookActorError::ValidationError(
                "Price is infinite".to_string(),
            ));
        } else if price <= 0.0 {
            log::warn!("{}: Price is non-positive ({})", self.name, price); // negative prices will exist in implied order books
            return Err(OrderBookActorError::ValidationError(
                "Price is not positive".to_string(),
            ));
        }

        let norm_price = (price / self.tick_size).round() * self.tick_size;
        Ok(OrderedF64(norm_price))
    }

    pub async fn process_message(&mut self, topic: String, data: serde_json::Value) {
        let mut resubscribe_needed = false;

        if let Err(err) = async {
            // Determine the type of the parsed message
            let message_type = match data["type"].as_str() {
                Some("change") => OrderBookMessageType::Change,
                Some("snapshot") => OrderBookMessageType::Snapshot,
                Some(other) => {
                    log::warn!("{}: Unknown message type: {}", self.name, other);
                    return Err(OrderBookActorError::ValidationError(format!(
                        "Unknown message type: {}",
                        other
                    )));
                }
                None => {
                    log::error!("{}: Missing 'type' field in message", self.name);
                    return Err(OrderBookActorError::MissingField(
                        "Missing 'type' field in message".to_string(),
                    ));
                }
            };

            // Process based on message type
            match message_type {
                OrderBookMessageType::Change => match self.process_update(&data).await {
                    ProcessMessageResult::Ok => {}
                    ProcessMessageResult::ErrRequiresResub(_) => {
                        resubscribe_needed = true;
                    }
                    ProcessMessageResult::ErrNoResub(err) => return Err(err),
                },
                OrderBookMessageType::Snapshot => match self.process_snapshot(&data).await {
                    ProcessMessageResult::Ok => {}
                    ProcessMessageResult::ErrRequiresResub(_) => {
                        resubscribe_needed = true;
                    }
                    ProcessMessageResult::ErrNoResub(err) => return Err(err),
                },
            }

            // Send processed data to BroadcastActor
            self.send_processed_data(topic).await?;

            Ok::<(), OrderBookActorError>(())
        }
        .await
        {
            log::error!("{}: Error processing message: {:?}", self.name, err);
        }

        // Send resubscribe command if necessary
        if resubscribe_needed {
            if let Err(send_err) = self.send_resubscribe_command().await {
                log::error!(
                    "{}: Failed to send resubscribe command: {:?}",
                    self.name,
                    send_err
                );
            }
        }
    }

    async fn process_snapshot(&mut self, data: &serde_json::Value) -> ProcessMessageResult {
        if let Err(err) = self.update_stored_change_id(data).await {
            log::error!(
                "{}: Error extracting change_id: {:?}, triggering resubscribe",
                self.name,
                err
            );
            return ProcessMessageResult::ErrRequiresResub(err);
        }

        // Insert snapshot bids/asks
        for side in &[OrderBookSide::Bids, OrderBookSide::Asks] {
            if let Some(updates) = data[side.as_str()].as_array() {
                let mut normalised_updates = Vec::new();
                for update in updates {
                    if let (Some(update_type_str), Some(price), Some(size)) =
                        (update[0].as_str(), update[1].as_f64(), update[2].as_f64())
                    {
                        if let Some(update_type) = OrderBookUpdate::from_str(update_type_str) {
                            match self.normalise_price(price).await {
                                Ok(normalised_price) => {
                                    normalised_updates.push((update_type, normalised_price, size));
                                }
                                Err(err) => {
                                    log::error!(
                                        "{}: Error normalising price: {:?}",
                                        self.name,
                                        err
                                    );
                                    return ProcessMessageResult::ErrRequiresResub(err);
                                }
                            }
                        } else {
                            log::error!("{}: Invalid update_type: {}", self.name, update_type_str);
                            return ProcessMessageResult::ErrRequiresResub(
                                OrderBookActorError::ValidationError(format!(
                                    "Invalid update_type: {}",
                                    update_type_str
                                )),
                            );
                        }
                    } else {
                        log::error!(
                            "{}: Malformed update entry in snapshot: {:?}",
                            self.name,
                            update
                        );
                        return ProcessMessageResult::ErrRequiresResub(
                            OrderBookActorError::ValidationError(
                                "Malformed update entry in snapshot".to_string(),
                            ),
                        );
                    }
                }

                let map = side.get_map(self);
                for (update_type, normalised_price, size) in normalised_updates {
                    match update_type {
                        OrderBookUpdate::New => {
                            map.insert(normalised_price, size);
                        }
                        _ => {
                            log::error!(
                                "Unexpected update_type in snapshot message: {}",
                                update_type
                            );
                        }
                    }
                }
            }
        }

        ProcessMessageResult::Ok
    }

    async fn check_prev_change_id(&self, data: &serde_json::Value) -> OrderBookResult<()> {
        if let Some(prev_change_id) = data.get("prev_change_id").and_then(|v| v.as_u64()) {
            if Some(prev_change_id) != self.change_id {
                log::error!(
                    "{}: prev_change_id mismatch. Expected: {:?}, Received: {}",
                    self.name,
                    self.change_id,
                    prev_change_id
                );
                return Err(OrderBookActorError::ValidationError(format!(
                    "prev_change_id mismatch. Expected: {:?}, Received: {}",
                    self.change_id, prev_change_id
                )));
            }
        } else {
            log::error!("{}: Missing prev_change_id field in data", self.name);
            return Err(OrderBookActorError::MissingField(
                "prev_change_id".to_string(),
            ));
        }

        Ok(())
    }

    async fn process_update(&mut self, data: &serde_json::Value) -> ProcessMessageResult {
        if let Err(err) = self.check_prev_change_id(data).await {
            log::error!(
                "{}: Could not validate prev_change_id on update: {:?}",
                self.name,
                err
            );
            return ProcessMessageResult::ErrRequiresResub(err);
        }

        if let Err(err) = self.update_stored_change_id(data).await {
            log::error!(
                "{}: Error extracting change_id: {:?}, triggering resubscribe",
                self.name,
                err
            );
            return ProcessMessageResult::ErrRequiresResub(err);
        }

        for side in &[OrderBookSide::Bids, OrderBookSide::Asks] {
            if let Some(updates) = data[side.as_str()].as_array() {
                let mut normalised_updates = Vec::new();
                for update in updates {
                    if let (Some(update_type_str), Some(price), Some(size)) =
                        (update[0].as_str(), update[1].as_f64(), update[2].as_f64())
                    {
                        if let Some(update_type) = OrderBookUpdate::from_str(update_type_str) {
                            match self.normalise_price(price).await {
                                Ok(normalised_price) => {
                                    normalised_updates.push((update_type, normalised_price, size));
                                }
                                Err(err) => {
                                    log::error!(
                                        "{}: Error normalising price in update: {:?}",
                                        self.name,
                                        err
                                    );
                                    return ProcessMessageResult::ErrRequiresResub(err);
                                }
                            }
                        } else {
                            log::error!("{}: Invalid update_type: {}", self.name, update_type_str);
                            return ProcessMessageResult::ErrRequiresResub(
                                OrderBookActorError::ValidationError(format!(
                                    "Invalid update_type: {}",
                                    update_type_str
                                )),
                            );
                        }
                    } else {
                        log::error!(
                            "{}: Malformed update entry, skipping: {:?}",
                            self.name,
                            update
                        );
                        return ProcessMessageResult::ErrRequiresResub(
                            OrderBookActorError::ValidationError(
                                "Malformed update entry in update message".to_string(),
                            ),
                        );
                    }
                }

                let map = side.get_map(self);
                for (update_type, normalised_price, size) in normalised_updates {
                    match update_type {
                        OrderBookUpdate::New | OrderBookUpdate::Change => {
                            map.insert(normalised_price, size);
                        }
                        OrderBookUpdate::Delete => {
                            map.remove(&normalised_price);
                        }
                    }
                }
            }
        }

        ProcessMessageResult::Ok
    }

    async fn send_resubscribe_command(&self) -> Result<(), OrderBookActorError> {
        if let Err(err) = self
            .ws_command_sender
            .send(WebSocketCommand::Resubscribe)
            .await
        {
            log::error!("{}: Failed to send resubscribe command: {}", self.name, err);
            return Err(OrderBookActorError::WebSocketCommandError(format!(
                "Failed to send resubscribe command: {}",
                err
            )));
        }
        log::info!("{}: Sent resubscribe command", self.name);

        Ok(())
    }

    async fn update_stored_change_id(
        &mut self,
        data: &serde_json::Value,
    ) -> Result<(), OrderBookActorError> {
        let change_id = data
            .get("change_id")
            .and_then(|v| v.as_u64())
            .ok_or_else(|| {
                OrderBookActorError::MissingField("Missing or invalid change_id".to_string())
            })?;

        self.change_id = Some(change_id);
        Ok(())
    }

    pub async fn send_processed_data(&self, topic: String) -> Result<(), OrderBookActorError> {
        let mut bids: Vec<(OrderedF64, f64)> = self
            .bids
            .iter()
            .map(|(&price, &size)| (price, size))
            .collect();
        bids.sort_by(|a, b| b.0.cmp(&a.0));

        let asks: Vec<(OrderedF64, f64)> = self
            .asks
            .iter()
            .map(|(&price, &size)| (price, size))
            .collect();

        let processed_data = ProcessedOrderBookData { topic, bids, asks };

        // Send to the broadcast channel
        if let Err(err) = self.broadcast.send(processed_data).await {
            log::error!(
                "{}: Failed to send ProcessedOrderBookData to broadcast: {}",
                self.name,
                err
            );
            return Err(OrderBookActorError::BroadcastError(format!(
                "Failed to send ProcessedOrderBookData to broadcast: {}",
                err
            )));
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn init_test_logger() {
        let _ = env_logger::builder().is_test(true).try_init();
    }

    #[tokio::test]
    async fn test_normalise_price() {
        init_test_logger();
        let (_, mock_parsed_data_receiver) = mpsc::channel(10); // Correctly pair sender and receiver
        let (mock_broadcast, _) = mpsc::channel(10);
        let (mock_ws_command_sender, _) = mpsc::channel(10);
        let config = StreamConfig::current();

        let order_book_actor = OrderBookActor {
            name: "TestOrderBookActor".to_string(),
            config,
            tick_size: 0.5,
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
            change_id: None,
            parsed_data_receiver: mock_parsed_data_receiver, // Use the receiver here
            broadcast: mock_broadcast,
            ws_command_sender: mock_ws_command_sender,
        };

        fn unwrap_price(res: OrderBookResult<OrderedF64>) -> f64 {
            match res {
                Ok(OrderedF64(p)) => p,
                Err(e) => panic!("Failed to normalise: {:?}", e),
            }
        }

        // Test cases
        assert_eq!(
            unwrap_price(order_book_actor.normalise_price(100.0).await),
            100.0
        );
        assert_eq!(
            unwrap_price(order_book_actor.normalise_price(100.3).await),
            100.5
        );
        assert_eq!(
            unwrap_price(order_book_actor.normalise_price(100.2).await),
            100.0
        );
        assert_eq!(
            unwrap_price(order_book_actor.normalise_price(99.8).await),
            100.0
        );
        assert_eq!(
            unwrap_price(order_book_actor.normalise_price(99.7).await),
            99.5
        );
        assert_eq!(
            unwrap_price(
                order_book_actor
                    .normalise_price(100.0000000000000000000001)
                    .await
            ),
            100.0
        );
        assert_eq!(
            unwrap_price(order_book_actor.normalise_price(1.2345e2).await),
            123.5
        );
        // Valid price
        assert_eq!(
            order_book_actor.normalise_price(100.7).await.unwrap().0,
            100.5
        );
        assert_eq!(
            order_book_actor.normalise_price(101.3).await.unwrap().0,
            101.5
        );
        // Invalid prices
        assert!(order_book_actor.normalise_price(f64::NAN).await.is_err());
        assert!(order_book_actor
            .normalise_price(f64::INFINITY)
            .await
            .is_err());
        assert!(order_book_actor.normalise_price(-100.0).await.is_err());
    }

    #[test]
    fn test_best_bid_and_ask() {
        init_test_logger();
        let (_, mock_parsed_data_receiver) = mpsc::channel(10); // Correct pairing
        let (mock_broadcast, _) = mpsc::channel(10);
        let (mock_ws_command_sender, _) = mpsc::channel(10);
        let config = StreamConfig::current();

        let mut actor = OrderBookActor {
            name: "TestOrderBookActor".to_string(),
            config,
            tick_size: 0.5,
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
            change_id: None,
            parsed_data_receiver: mock_parsed_data_receiver, // Use the receiver here
            broadcast: mock_broadcast,
            ws_command_sender: mock_ws_command_sender,
        };

        // Empty order book
        assert_eq!(actor.best_bid(), None);
        assert_eq!(actor.best_ask(), None);

        // Non-empty order book
        actor.bids.insert(OrderedF64(100.0), 10.0);
        actor.asks.insert(OrderedF64(101.0), 5.0);
        assert_eq!(actor.best_bid(), Some((100.0, 10.0)));
        assert_eq!(actor.best_ask(), Some((101.0, 5.0)));
    }

    #[tokio::test]
    async fn test_send_processed_data() {
        init_test_logger();
        let (data_sender, mut data_receiver) = mpsc::channel(10);
        let (_, mock_parsed_data_receiver) = mpsc::channel(10); // Mock ParsedMessage channel
        let config = StreamConfig::current();

        let mut actor = OrderBookActor::new(
            "TestOrderBookActor",
            &config,
            mock_parsed_data_receiver, // Provide the mock receiver
            data_sender,
            mpsc::channel(10).0,
        );

        // Populate the order book
        actor.bids.insert(OrderedF64(30000.0), 1.0);
        actor.asks.insert(OrderedF64(30001.0), 1.5);

        // Send processed data
        let topic = format!("{}.{}", config.internal_symbol, config.requested_feed);
        actor.send_processed_data(topic.clone()).await.unwrap();

        // Verify the data sent to the channel
        if let Some(processed_data) = data_receiver.recv().await {
            log::info!("Processed data received: {:?}", processed_data); // Log the processed data
            assert_eq!(processed_data.topic, topic); // Ensure topic matches
            assert_eq!(processed_data.bids, vec![(OrderedF64(30000.0), 1.0)]);
            assert_eq!(processed_data.asks, vec![(OrderedF64(30001.0), 1.5)]);
        } else {
            panic!("No data received in the broadcast channel");
        }
    }

    #[tokio::test]
    async fn test_send_processed_data_empty_order_book() {
        init_test_logger();
        let (data_sender, mut data_receiver) = mpsc::channel(10);
        let config = StreamConfig::current();

        let actor = OrderBookActor::new(
            "TestOrderBookActor",
            &config,
            mpsc::channel(10).1,
            data_sender,
            mpsc::channel(10).0,
        );

        // Send processed data with an empty order book
        let topic = format!("{}.{}", config.internal_symbol, config.requested_feed);
        actor.send_processed_data(topic).await.unwrap();

        let topic = format!("{}.{}", config.internal_symbol, config.requested_feed);

        // Verify the data sent to the channel
        if let Some(processed_data) = data_receiver.recv().await {
            assert_eq!(processed_data.topic, topic);
            assert!(processed_data.bids.is_empty());
            assert!(processed_data.asks.is_empty());
        } else {
            panic!("No data received in the broadcast channel");
        }
    }

    #[tokio::test]
    async fn test_send_processed_data_channel_closed() {
        init_test_logger();
        let (data_sender, _) = mpsc::channel(10); // Drop the receiver to simulate channel closure
        let config = StreamConfig::current();

        let mut actor = OrderBookActor::new(
            "TestOrderBookActor",
            &config,
            mpsc::channel(10).1,
            data_sender,
            mpsc::channel(10).0,
        );

        // Populate the order book
        actor.bids.insert(OrderedF64(30000.0), 1.0);
        actor.asks.insert(OrderedF64(30001.0), 1.5);

        // Attempt to send processed data
        let topic = format!("{}.{}", config.internal_symbol, config.requested_feed);
        let result = actor.send_processed_data(topic).await;

        // Verify that an error is returned
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_empty_parsed_message() {
        init_test_logger();
        let (_, mock_parsed_data_receiver) = mpsc::channel(10);
        let (mock_broadcast, _) = mpsc::channel(10);
        let (mock_ws_command_sender, _) = mpsc::channel(10);
        let config = StreamConfig::current();

        let mut actor = OrderBookActor::new(
            "TestOrderBookActor",
            &config,
            mock_parsed_data_receiver,
            mock_broadcast,
            mock_ws_command_sender,
        );

        // Simulate an empty ParsedMessage
        let message = ParsedMessage::OrderBook {
            topic: String::new(),
            data: serde_json::Value::Null,
        };

        if let ParsedMessage::OrderBook { topic, data } = message {
            actor.process_message(topic, data).await;
        }

        // Assert no changes to the order book
        assert_eq!(actor.bids.len(), 0);
        assert_eq!(actor.asks.len(), 0);
    }

    #[tokio::test]
    async fn test_send_processed_data_sorted() {
        let (data_sender, mut data_receiver) = mpsc::channel(10);
        let (_, mock_parsed_data_receiver) = mpsc::channel(10); // Mock ParsedMessage channel
        let config = StreamConfig::current();

        let mut actor = OrderBookActor::new(
            "TestOrderBookActor",
            &config,
            mock_parsed_data_receiver,
            data_sender,
            mpsc::channel(10).0,
        );

        // Populate the order book
        actor.bids.insert(OrderedF64(101.0), 5.0);
        actor.bids.insert(OrderedF64(102.0), 3.0);
        actor.bids.insert(OrderedF64(100.0), 2.0);

        actor.asks.insert(OrderedF64(103.0), 2.0);
        actor.asks.insert(OrderedF64(104.0), 1.0);
        actor.asks.insert(OrderedF64(102.0), 4.0);

        // Send processed data
        let topic = format!("{}.{}", config.internal_symbol, config.requested_feed);
        actor.send_processed_data(topic.clone()).await.unwrap();

        // Verify the data sent to the channel
        if let Some(processed_data) = data_receiver.recv().await {
            assert_eq!(processed_data.topic, topic);

            // Ensure bids are sorted in descending order
            assert_eq!(
                processed_data.bids,
                vec![
                    (OrderedF64(102.0), 3.0),
                    (OrderedF64(101.0), 5.0),
                    (OrderedF64(100.0), 2.0)
                ]
            );

            // Ensure asks are sorted in ascending order
            assert_eq!(
                processed_data.asks,
                vec![
                    (OrderedF64(102.0), 4.0),
                    (OrderedF64(103.0), 2.0),
                    (OrderedF64(104.0), 1.0)
                ]
            );
        } else {
            panic!("No data received in the broadcast channel");
        }
    }
}
