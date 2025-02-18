use crate::async_actors::common::RequestedFeed;
use crate::async_actors::messages::{ExchangeMessage, RouterMessage, RouterCommand, MarketData};
use crate::async_actors::deribit::websocket::SubscriptionManagementAction;
use crate::async_actors::subscription::{ExchangeSubscription, SubscriptionInfo};
use tokio::sync::mpsc;
use tokio::time::{self, Duration};
use tracing::{debug, error, info, Instrument, warn};
use thiserror::Error;
use tungstenite::Message;
use std::collections::HashMap;

const ROUTER_HEARTBEAT_INTERVAL: u64 = 5;

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
    #[error("Message type from Websocket must be valid text or binary")]
    InvalidMessageType,
    #[error("Field found but invalid type: {0}")]
    InvalidType(serde_json::Value),
    #[error("Unknown subscription management ID, message: {0}")]
    InvalidSubscriptionManagementMessage(serde_json::Value)
}

type ParseMessageResult<T> = Result<T, RouterParseError>;

enum ParsedMessage {
    MarketData {
        exchange_stream_id: String,
        data: serde_json::Value
    },
    Subscribe {
        channels: Vec<String>,
    },
    Unsubscribe {
        channels: Vec<String>,
    },
    UnsubscribeAll {
        result: String,
    },
    Test {
        version: String,
    }
}

pub struct DeribitRouterActor {
    actor_id: String,
    router_receiver: mpsc::Receiver<ExchangeMessage>,
    to_orch: mpsc::Sender<RouterMessage>,
    from_orch: mpsc::Receiver<RouterCommand>,
    registered_streams: HashMap<String, ExchangeSubscription>,
}

impl DeribitRouterActor {
    pub fn new(
        actor_id: String,
        router_receiver: mpsc::Receiver<ExchangeMessage>,
        to_orch: mpsc::Sender<RouterMessage>,
        from_orch: mpsc::Receiver<RouterCommand>,
    ) -> Self {
        let registered_streams = HashMap::new();
        Self {
            actor_id,
            router_receiver,
            to_orch,
            from_orch,
            registered_streams,
        }
    }

    pub async fn run(mut self) {
        let span = tracing::info_span!(
            "DeribitRouterActor",
            actor_id = %self.actor_id
        );

        async move {
            let mut heartbeat_interval =
                time::interval(Duration::from_secs(ROUTER_HEARTBEAT_INTERVAL));
            loop {
                tokio::select! {

                    _ = heartbeat_interval.tick() => {
                        if !self.send_heartbeat().await {
                            // TODO review the behaviour, what should we do if we fail to heartbeat?
                            break;
                        }
                    }
                    
                    Some(exchange_message) = self.router_receiver.recv() => {
                        match self.parse_message(&exchange_message).await {
                            Ok(parsed_message) => {
                                match parsed_message {
                                    ParsedMessage::MarketData { exchange_stream_id, data } => {
                                        self.handle_market_data_message(exchange_stream_id, data).await;
                                    }
                                    ParsedMessage::Subscribe { channels } => {
                                        let ws_actor_id = &exchange_message.actor_id.clone();
                                        self.send_subscription_confirm(
                                            channels,
                                            ws_actor_id,
                                            SubscriptionManagementAction::Subscribe
                                        ).await;
                                    }
                                    ParsedMessage::Unsubscribe { channels } => {
                                        let ws_actor_id = &exchange_message.actor_id.clone();
                                        self.send_subscription_confirm(
                                            channels,
                                            ws_actor_id,
                                            SubscriptionManagementAction::Unsubscribe
                                        ).await;
                                    }
                                    ParsedMessage::UnsubscribeAll { result } => {
                                        info!("✅ Received UnsubscribeAll confirmation: {}", result); // TODO implement forward handling
                                    }
                                    ParsedMessage::Test { version } => {
                                        debug!("✅ Test response confirmed, Deribit API version: {}", version); // TODO what should do here, if anything?
                                    }
                                }
                            },
                            Err(e) => warn!("⚠️ Failed to parse exchange message: {:?}", e),
                        }
                    }

                    Some(command) = self.from_orch.recv() => {
                        self.handle_router_command(command);
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
            .send(RouterMessage::Heartbeat {
                actor_id: self.actor_id.clone(),
            })
            .await
        {
            Ok(_) => {
                debug!(actor_id = %self.actor_id, "Sent hearbeat");
                true
            }
            Err(e) => {
                error!(actor_id = %self.actor_id, "Failed to send heartbeat to Orchestrator: {e}");
                false
            }
        }
    }

    async fn parse_message(
        &mut self,
        exchange_message: &ExchangeMessage
    ) -> ParseMessageResult<ParsedMessage> {
        let text = match &exchange_message.message {
            Message::Text(text) => text.to_string(),
            Message::Binary(bin) => String::from_utf8(bin.clone().into())
                .map_err(|_| RouterParseError::InvalidMessageType)?,
            _ => {
                warn!("❌ Received unsupported message type: {:?}", exchange_message);
                return Err(RouterParseError::InvalidMessageType);
            }
        };
        let parsed_message: serde_json::Value = serde_json::from_str(&text)?;
        
        // ✅ Market Data Handling
        if let Some(params) = parsed_message.get("params") {
            let exchange_stream_id = params.get("channel")
                .and_then(|v| v.as_str())
                .ok_or_else(||RouterParseError::MissingField("params.channel".to_string()))?
                .to_string();

            let data = params.get("data")
                .cloned()
                .ok_or_else(||RouterParseError::MissingField("params.data".to_string()))?;

            return Ok(ParsedMessage::MarketData { exchange_stream_id, data});
        }
        
        // ✅ Subscription Management Handling
        let id = parsed_message.get("id")
            .and_then(|v| v.as_u64())
            .ok_or_else(||RouterParseError::MissingField("message.id".to_string()))?;

        match SubscriptionManagementAction::from_id(id) {
            Some(SubscriptionManagementAction::Subscribe) => {
                let channels = parsed_message.get("result")
                    .and_then(|v| v.as_array())
                    .map(|arr| arr.iter().filter_map(|s| s.as_str().map(String::from)).collect())
                    .ok_or_else(|| RouterParseError::MissingField("result".to_string()))?;
                return Ok(ParsedMessage::Subscribe { channels });
            }
            Some(SubscriptionManagementAction::Unsubscribe) => {
                let channels = parsed_message.get("result")
                    .and_then(|v| v.as_array())
                    .map(|arr| arr.iter().filter_map(|s| s.as_str().map(String::from)).collect())
                    .ok_or_else(|| RouterParseError::MissingField("result".to_string()))?;
                return Ok(ParsedMessage::Unsubscribe { channels });
            }
            Some(SubscriptionManagementAction::UnsubscribeAll) => {
                let result = parsed_message.get("result")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| RouterParseError::MissingField("result".to_string()))?
                    .to_string();
                return Ok(ParsedMessage::UnsubscribeAll { result })
            }
            Some(SubscriptionManagementAction::Test) => {
                if let Some(result) = parsed_message.get("result") {
                    let version = result.get("version")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| RouterParseError::MissingField("version".to_string()))?
                    .to_string();
                    return Ok(ParsedMessage::Test { version })
                } else {
                    return Err(RouterParseError::MissingField("result".to_string()))
                }  
            }
            None => {
                warn!("⚠️ Unknown subscription management action ID: {}", id);
                return Err(RouterParseError::InvalidSubscriptionManagementMessage(parsed_message));
            }
        }
    }

    async fn handle_market_data_message(
        &mut self,
        exchange_stream_id: String,
        data: serde_json::Value
    ) {

        // ✅ 1️⃣ Lookup the subscription using `exchange_stream_id`
        let Some(subscription) = self.registered_streams.get(&exchange_stream_id) else {
            warn!(
                "⚠️ Received market data for unknown exchange_stream_id: {}",
                exchange_stream_id
            );
            return;
        };
        
        // ✅ 2️⃣ Extract internal stream ID for forwarding
        let topic = subscription.stream_id().to_string();

        // ✅ 3️⃣ Construct market data message
        let _ = MarketData{topic: topic, data};

        match subscription.feed_type() {
            RequestedFeed::OrderBook => {
                // TODO forward to relevant data processing actor
            }
        }
    }

    fn handle_router_command(
        &mut self,
        command: RouterCommand
    ) {
        match command {
            RouterCommand::Register { subscription } => {
                let exchange_stream_id = subscription.exchange_stream_id().to_string();
                let stream_id = subscription.stream_id().to_string();
                if self.registered_streams.contains_key(&exchange_stream_id) {
                    warn!("Trying to register duplicate stream ID: {}", stream_id);
                } else {
                    self.registered_streams.insert(
                        exchange_stream_id,
                        subscription
                    );
                    info!("Registered subscription: {}", stream_id);
                }
                
            }
            RouterCommand::Remove { subscription } => {
                let exchange_stream_id = subscription.exchange_stream_id().to_string();
                if self.registered_streams.remove(&exchange_stream_id).is_some() {
                    info!("Removed subscription: {}", subscription.stream_id());
                } else {
                    warn!("Tried to remove non-existent subscription: {}", subscription.stream_id());
                }
            }
        }
    }

    async fn send_subscription_confirm(
        &self,
        channels: Vec<String>,
        ws_actor_id: &String,
        action: SubscriptionManagementAction,
    ) {
        
        // ✅ Ensure there are valid channels
        if channels.is_empty() {
            warn!("⚠️ Subscription confirmation received with empty channels list.");
            return;
        }

      for raw_channel in channels {
        info!("📡 Forwarding subscription confirmation for: {} (Action: {})", raw_channel, action.as_str());

        info!("Forwarding subscription management confirmation for: {raw_channel} to Orchestrator from: {}", action.as_str());
        match extract_feed_and_symbol(&raw_channel) {
            Ok((feed_type, exchange_symbol)) => {
                let message = match action {
                    SubscriptionManagementAction::Subscribe => {
                        RouterMessage::ConfirmSubscribe{
                            ws_actor_id: ws_actor_id.to_string(),
                            exchange_symbol,
                            feed_type
                        }
                    },
                    SubscriptionManagementAction::Unsubscribe => {
                        RouterMessage::ConfirmUnsubscribe {
                            ws_actor_id: ws_actor_id.to_string(),
                            exchange_symbol,
                            feed_type
                        }
                    },
                    _ => {
                        error!("❌ send_subscription_confirm received unsupported action: {:?}", action);
                        continue;
                    },
                };
                if let Err(e) = self.to_orch.send(message).await {
                    error!("❌ Failed to send RouterMessage: {:?}, Error: {:?}", action, e);
                }
            }
            Err(e) => {
                warn!("⚠️ Skipping invalid subscription format: {:?}", e);
            }
        }
      }  
    }
}

fn extract_feed_and_symbol(raw_channel: &str) -> ParseMessageResult<(RequestedFeed, String)> {
    let parts: Vec<&str> = raw_channel.split(".").collect();
    if parts.len() < 2 {
        return Err(RouterParseError::UnknownChannel(format!("Invalid channel format: {}", raw_channel)))
    }
    let feed_type = RequestedFeed::from_exchange_str(parts[0])
        .ok_or_else(||RouterParseError::UnknownChannel(parts[0].to_string()))?;
    let exchange_symbol = parts[1].to_string();
    Ok((feed_type, exchange_symbol))
}