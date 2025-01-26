use super::ws::SubscriptionManagementAction;
use log;
use serde_json;
use std::collections::HashMap;
use thiserror::Error;
use tokio::sync::mpsc;

#[derive(Debug)]
struct SubscriptionManagementResponse {
    channels: Vec<String>,
}

impl SubscriptionManagementResponse {
    pub fn new(parsed_message: &serde_json::Value) -> Result<Self, RouterParseError> {
        let result = parsed_message["result"]
            .as_array()
            .ok_or_else(|| RouterParseError::MissingField("message.result".to_string()))?;

        let channels = result
            .iter()
            .filter_map(|c| c.as_str().map(|s| s.to_string()))
            .collect();

        Ok(SubscriptionManagementResponse { channels })
    }

    // Return comma-seperated string of channel names for logging
    pub fn channel_log_string(&self) -> String {
        self.channels.join(",")
    }

    pub fn channels(&self) -> &Vec<String> {
        &self.channels
    }
}

#[derive(Debug, Error)]
pub enum RouterParseError {
    #[error("Failed to parse JSON: {0}")]
    JsonParseError(#[from] serde_json::Error),
    #[error("Missing or invalid field: {0}")]
    MissingField(String),
    #[error("Unknown channel: {0}")]
    UnknownChannel(String),
    #[error("Failed to send parsed message")]
    FailToSendParsedMessage(String),
}

type ParseMessageResult<T> = Result<T, RouterParseError>;

#[derive(Debug)]
pub enum ChannelType {
    OrderBook,
    Trades,
}

impl ChannelType {
    // maps from a channel to a 'FeedType'
    pub fn from_channel(channel: &str) -> Result<Self, RouterParseError> {
        // Split the channel into parts
        let parts: Vec<&str> = channel.split(".").collect();

        // Ensure the channel has at least one part
        if parts.is_empty() {
            return Err(RouterParseError::UnknownChannel(channel.to_string()));
        }

        // Match on the channel prefix
        let prefix = parts[0];
        match prefix {
            "book" => Ok(ChannelType::OrderBook),
            "trades" => Ok(ChannelType::Trades),
            _ => Err(RouterParseError::UnknownChannel(channel.to_string())),
        }
    }
}

#[derive(Debug)]
pub enum ParsedMessage {
    OrderBook {
        topic: String,
        data: serde_json::Value,
    },
    Trades {
        topic: String,
        data: serde_json::Value,
    },
}

pub struct DeribitMessageRouter {
    ws_router_receiver: mpsc::Receiver<String>,
    parsed_data_sender: mpsc::Sender<ParsedMessage>,
    topic_map: HashMap<String, String>,
    subscriptions: HashMap<String, bool>,
}

impl DeribitMessageRouter {
    pub fn new(
        ws_router_receiver: mpsc::Receiver<String>,
        parsed_data_sender: mpsc::Sender<ParsedMessage>,
    ) -> Self {
        // Hardcoded topic mapping while still in dev
        let mut topic_map = HashMap::new();
        topic_map.insert(
            "book.BTC-PERPETUAL.100ms".to_string(),
            "Deribit.InvFut.BTC.USD.OrderBook".to_string(),
        );
        let subscriptions = HashMap::new();

        Self {
            ws_router_receiver,
            parsed_data_sender,
            topic_map,
            subscriptions,
        }
    }

    pub async fn run(&mut self) {
        log::info!("Starting DeribitMessageRouter");
        while let Some(raw_message) = self.ws_router_receiver.recv().await {
            if let Err(err) = self.parse_message(&raw_message).await {
                log::warn!(
                    "Failed tp process raw message: {:?}, message: {}",
                    err,
                    raw_message
                );
            }
        }
        log::error!("DeribitMessageRouter shutting down: receiver closed");
    }

    pub async fn parse_message(&mut self, message: &str) -> ParseMessageResult<()> {
        // Parse the message as JSON
        let parsed_message: serde_json::Value =
            serde_json::from_str(message).map_err(RouterParseError::JsonParseError)?;

        if parsed_message.get("params").is_some() {
            self.handle_stream_message(parsed_message).await?;
        } else {
            self.handle_subscription_management_message(parsed_message)
                .await?;
        }
        Ok(())
    }

    pub async fn handle_stream_message(
        &self,
        parsed_message: serde_json::Value,
    ) -> ParseMessageResult<()> {
        // Extract the params object
        let params = parsed_message["params"]
            .as_object()
            .ok_or_else(|| RouterParseError::MissingField("message.params".to_string()))?;

        // Extract the channel
        let channel = params
            .get("channel")
            .and_then(|c| c.as_str())
            .ok_or_else(|| RouterParseError::MissingField("params.channel".to_string()))?;

        // Determine feed type based on the channel and return appropiate enum
        let channel_type = ChannelType::from_channel(channel)?;

        // Map the exchange topic to an internal topic
        let topic = self
            .topic_map
            .get(channel)
            .ok_or_else(|| RouterParseError::UnknownChannel(channel.to_string()))?
            .clone();

        // Extract and validate the data payload
        let data = params
            .get("data")
            .cloned()
            .ok_or_else(|| RouterParseError::MissingField("params.data".to_string()))?;

        match channel_type {
            ChannelType::OrderBook => {
                self.send_parsed_message(ParsedMessage::OrderBook { topic, data })
                    .await
            }
            ChannelType::Trades => {
                self.send_parsed_message(ParsedMessage::Trades { topic, data })
                    .await
            }
        }
    }

    pub async fn send_parsed_message(
        &self,
        parsed_message: ParsedMessage,
    ) -> Result<(), RouterParseError> {
        self.parsed_data_sender
            .send(parsed_message)
            .await
            .map_err(|err| {
                log::error!("Failed to send parsed message: {}", err);
                RouterParseError::FailToSendParsedMessage(
                    "Failed to send parsed message".to_string(),
                )
            })?;

        Ok(())
    }

    async fn handle_subscription_management_message(
        &mut self,
        parsed_message: serde_json::Value,
    ) -> ParseMessageResult<()> {
        let id = parsed_message["id"]
            .as_u64()
            .ok_or_else(|| RouterParseError::MissingField("message.id".to_string()))?;
        // let sub_mgt_msg = SubscriptionManagementResponse::new(&parsed_message)?;

        match SubscriptionManagementAction::from_id(id) {
            Some(SubscriptionManagementAction::Subscribe) => {
                let sub_mgt_msg = SubscriptionManagementResponse::new(&parsed_message)?;
                for channel in sub_mgt_msg.channels() {
                    self.subscriptions.insert(channel.clone(), true);
                }
                log::info!(
                    "Subscribed to channels: {}",
                    sub_mgt_msg.channel_log_string()
                );
            }
            Some(SubscriptionManagementAction::Unsubscribe) => {
                let sub_mgt_msg = SubscriptionManagementResponse::new(&parsed_message)?;
                for channel in sub_mgt_msg.channels() {
                    self.subscriptions.insert(channel.clone(), false);
                }
                log::info!(
                    "Unsubscribed from channels: {}",
                    sub_mgt_msg.channel_log_string()
                )
            }
            Some(SubscriptionManagementAction::Test) => {
                log::debug!("Received test response")
            }
            None => {
                log::warn!(
                    "Unknown subscription action ID: {} in message {}",
                    id,
                    parsed_message
                )
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use env_logger;
    use tokio::sync::mpsc;

    fn init_test_logger() {
        let _ = env_logger::builder().is_test(true).try_init();
    }

    fn create_test_router() -> (DeribitMessageRouter, mpsc::Sender<String>) {
        let (ws_router_sender, ws_router_receiver) = mpsc::channel(10);
        let (parsed_data_sender, _parsed_data_receiver) = mpsc::channel(10);

        let router = DeribitMessageRouter::new(ws_router_receiver, parsed_data_sender);
        (router, ws_router_sender)
    }

    #[tokio::test]
    async fn test_unknown_channel() {
        init_test_logger();
        let (mut router, sender) = create_test_router();

        // Simulate a message with an unknown channel
        let raw_message = r#"{
            "params": {
                "channel": "unknown.channel",
                "data": { "field": "value" }
            }
        }"#
        .to_string();

        // Send the message to the router
        sender.send(raw_message).await.unwrap();

        // Run the router
        tokio::spawn(async move {
            router.run().await;
        })
        .await
        .unwrap();

        // Check logs (manual verification during test execution)
        log::info!("Test for unknown channel completed.");
    }

    #[tokio::test]
    async fn test_invalid_json_structure() {
        init_test_logger();
        let (mut router, sender) = create_test_router();

        // Simulate an invalid JSON message
        let raw_message = r#"{
            "invalid_json": "missing closing brace"
        "#
        .to_string();

        // Send the message to the router
        sender.send(raw_message).await.unwrap();

        // Run the router
        tokio::spawn(async move {
            router.run().await;
        })
        .await
        .unwrap();

        // Check logs (manual verification during test execution)
        log::info!("Test for invalid JSON structure completed.");
    }

    #[tokio::test]
    async fn test_missing_required_fields() {
        init_test_logger();
        let (mut router, sender) = create_test_router();

        // Simulate a message missing required fields
        let raw_message = r#"{
            "params": {
                "data": { "field": "value" }
                // "channel" is missing
            }
        }"#
        .to_string();

        // Send the message to the router
        sender.send(raw_message).await.unwrap();

        // Run the router
        tokio::spawn(async move {
            router.run().await;
        })
        .await
        .unwrap();

        // Check logs (manual verification during test execution)
        log::info!("Test for missing required fields completed.");
    }
}

#[cfg(test)]
mod subscription_management_response_tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_valid_subscription_response() {
        let valid_response = json!({
            "result": [
                "deribit_price_index.btc_usd",
                "book.BTC-PERPETUAL.100ms"
            ]
        });

        let response = SubscriptionManagementResponse::new(&valid_response).unwrap();
        assert_eq!(
            response.channels,
            vec![
                "deribit_price_index.btc_usd".to_string(),
                "book.BTC-PERPETUAL.100ms".to_string()
            ]
        );
        assert_eq!(
            response.channel_log_string(),
            "deribit_price_index.btc_usd,book.BTC-PERPETUAL.100ms"
        );
    }

    #[test]
    fn test_missing_id_in_response() {
        let invalid_response = serde_json::json!({
            "result": [
                "deribit_price_index.btc_usd"
            ]
        });

        let response = SubscriptionManagementResponse::new(&invalid_response);
        assert!(response.is_ok()); // No longer checks for id
        let response = response.unwrap();
        assert_eq!(
            response.channels,
            vec!["deribit_price_index.btc_usd".to_string()]
        );
    }

    #[test]
    fn test_missing_result_in_response() {
        let invalid_response = json!({});

        let response = SubscriptionManagementResponse::new(&invalid_response);
        assert!(response.is_err());
        if let Err(RouterParseError::MissingField(field)) = response {
            assert_eq!(field, "message.result");
        } else {
            panic!("Expected MissingField error for result");
        }
    }

    #[test]
    fn test_empty_result_array() {
        let valid_response = json!({
            "result": []
        });

        let response = SubscriptionManagementResponse::new(&valid_response).unwrap();
        assert_eq!(response.channels, Vec::<String>::new());
        assert_eq!(response.channel_log_string(), "");
    }
}

#[cfg(test)]
mod router_subscription_management_tests {
    use super::*;
    use serde_json::json;
    use tokio::sync::mpsc;

    fn create_test_router() -> (DeribitMessageRouter, mpsc::Sender<String>) {
        let (ws_router_sender, ws_router_receiver) = mpsc::channel(10);
        let (parsed_data_sender, _parsed_data_receiver) = mpsc::channel(10);

        let router = DeribitMessageRouter::new(ws_router_receiver, parsed_data_sender);
        (router, ws_router_sender)
    }

    #[tokio::test]
    async fn test_handle_subscription_message_subscribe() {
        let (mut router, _) = create_test_router();

        let subscription_message = json!({
            "id": 1,
            "result": [
                "book.BTC-PERPETUAL.100ms",
                "trades.BTC-PERPETUAL"
            ]
        });

        let result = router
            .handle_subscription_management_message(subscription_message)
            .await;
        assert!(result.is_ok());
        assert_eq!(
            router.subscriptions.get("book.BTC-PERPETUAL.100ms"),
            Some(&true)
        );
        assert_eq!(
            router.subscriptions.get("trades.BTC-PERPETUAL"),
            Some(&true)
        );
    }

    #[tokio::test]
    async fn test_handle_subscription_message_unsubscribe() {
        let (mut router, _) = create_test_router();

        router
            .subscriptions
            .insert("book.BTC-PERPETUAL.100ms".to_string(), true);
        router
            .subscriptions
            .insert("trades.BTC-PERPETUAL".to_string(), true);

        let unsubscription_message = json!({
            "id": 2,
            "result": [
                "book.BTC-PERPETUAL.100ms"
            ]
        });

        let result = router
            .handle_subscription_management_message(unsubscription_message)
            .await;
        assert!(result.is_ok());
        assert_eq!(
            router.subscriptions.get("book.BTC-PERPETUAL.100ms"),
            Some(&false)
        );
        assert_eq!(
            router.subscriptions.get("trades.BTC-PERPETUAL"),
            Some(&true)
        );
    }

    #[tokio::test]
    async fn test_unknown_subscription_action_id() {
        let (mut router, _) = create_test_router();

        let unknown_action_message = json!({
            "id": 999,
            "result": [
                "book.BTC-PERPETUAL.100ms"
            ]
        });

        let result = router
            .handle_subscription_management_message(unknown_action_message)
            .await;
        assert!(result.is_ok()); // Should not error, just warn
        assert!(router.subscriptions.is_empty());
    }

    #[tokio::test]
    async fn test_missing_id_in_subscription_message() {
        let (mut router, _) = create_test_router();

        let invalid_message = json!({
            "result": [
                "book.BTC-PERPETUAL.100ms"
            ]
        });

        let result = router
            .handle_subscription_management_message(invalid_message)
            .await;
        assert!(result.is_err());
        if let Err(RouterParseError::MissingField(field)) = result {
            assert_eq!(field, "message.id");
        } else {
            panic!("Expected MissingField error for id");
        }
    }

    #[tokio::test]
    async fn test_missing_result_in_subscription_message() {
        let (mut router, _) = create_test_router();

        let invalid_message = json!({
            "id": 1
        });

        let result = router
            .handle_subscription_management_message(invalid_message)
            .await;
        assert!(result.is_err());
        if let Err(RouterParseError::MissingField(field)) = result {
            assert_eq!(field, "message.result");
        } else {
            panic!("Expected MissingField error for result");
        }
    }
}
