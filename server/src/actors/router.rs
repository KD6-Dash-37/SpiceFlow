use log;
use serde_json;
use std::collections::HashMap;
use thiserror::Error;
use tokio::sync::mpsc;

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

        Self {
            ws_router_receiver,
            parsed_data_sender,
            topic_map,
        }
    }

    pub async fn run(&mut self) {
        log::info!("Starting DeribitMessageRouter");
        while let Some(raw_message) = self.ws_router_receiver.recv().await {
            match self.parse_message(&raw_message) {
                Ok(parsed_message) => {
                    if let Err(err) = self.send_parsed_message(parsed_message).await {
                        log::error!("Failed to send parsed message: {:?}", err);
                    }
                }
                Err(err) => {
                    log::warn!(
                        "Failed to process raw message: {:?}. Message: {}",
                        err,
                        raw_message
                    );
                }
            }
        }
        log::error!("DeribitMessageRouter shutting down: receiver closed");
    }

    pub fn parse_message(&self, message: &str) -> ParseMessageResult<ParsedMessage> {
        // Parse the message as JSON
        let parsed_message: serde_json::Value =
            serde_json::from_str(message).map_err(RouterParseError::JsonParseError)?;

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
            ChannelType::OrderBook => Ok(ParsedMessage::OrderBook { topic, data }),
            ChannelType::Trades => Ok(ParsedMessage::Trades { topic, data }),
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
