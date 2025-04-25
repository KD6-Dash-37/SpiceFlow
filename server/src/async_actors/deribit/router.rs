// server/src/async_actors/deribit/router.rs

// 🌍 Standard library
use std::collections::HashMap;

// 📦 External Crates
use thiserror::Error;
use tokio::sync::mpsc;
use tokio::time::{self, Duration};
use tracing::{debug, error, info, warn, Instrument};
use tungstenite::Message;

// 🧠 Internal Crates / Modules
use crate::async_actors::messages::{ExchangeMessage, RawMarketData, RouterCommand, RouterMessage};
use crate::async_actors::websocket::SubscriptionManagementAction;
use crate::domain::ExchangeSubscription;
use crate::model::RequestedFeed;

const ROUTER_HEARTBEAT_INTERVAL: u64 = 5;

#[allow(clippy::module_name_repetitions)]
#[derive(Debug, Error)]
pub enum RouterParseError {
    #[error("Failed to parse JSON: {0}")]
    JsonParseError(#[from] serde_json::Error),
    #[error("Missing or invalid field: {0}")]
    MissingField(String),
    #[error("Unknown channel: {0}")]
    UnknownChannel(String),
    #[error("Message type from Websocket must be valid text or binary")]
    InvalidMessageType,
    #[error("Unknown subscription management ID, message: {0}")]
    InvalidSubscriptionManagementMessage(serde_json::Value),
}

type ParseMessageResult<T> = Result<T, RouterParseError>;

enum ParsedMessage {
    MarketData {
        exchange_stream_id: String,
        data: serde_json::Value,
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
    },
}

pub struct DeribitRouterActor {
    actor_id: String,
    router_receiver: mpsc::Receiver<ExchangeMessage>,
    to_orch: mpsc::Sender<RouterMessage>,
    from_orch: mpsc::Receiver<RouterCommand>,
    registered_streams: HashMap<String, ExchangeSubscription>,
    to_data_processor: HashMap<String, mpsc::Sender<RawMarketData>>,
}

impl DeribitRouterActor {
    #[must_use]
    pub fn new(
        actor_id: String,
        router_receiver: mpsc::Receiver<ExchangeMessage>,
        to_orch: mpsc::Sender<RouterMessage>,
        from_orch: mpsc::Receiver<RouterCommand>,
    ) -> Self {
        let registered_streams = HashMap::new();
        let to_data_processor = HashMap::new();
        Self {
            actor_id,
            router_receiver,
            to_orch,
            from_orch,
            registered_streams,
            to_data_processor,
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
                        match parse_message(&exchange_message) {
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
                                        debug!("✅ Received UnsubscribeAll confirmation: {}", result); // TODO implement forward handling
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
        .await;
    }

    async fn send_heartbeat(&self) -> bool {
        match self
            .to_orch
            .send(RouterMessage::Heartbeat {
                actor_id: self.actor_id.clone(),
            })
            .await
        {
            Ok(()) => {
                debug!("Sent hearbeat");
                true
            }
            Err(e) => {
                error!("Failed to send heartbeat to Orchestrator: {e}");
                false
            }
        }
    }

    async fn handle_market_data_message(
        &mut self,
        exchange_stream_id: String,
        data: serde_json::Value,
    ) {
        let Some(subscription) = self.registered_streams.get(&exchange_stream_id) else {
            warn!(
                "Received market data for unknown exchange_stream_id: {}",
                exchange_stream_id
            );
            return;
        };

        let market_data = RawMarketData {
            stream_id: subscription.stream_id.clone(),
            data,
        };

        match subscription.requested_feed {
            RequestedFeed::OrderBook => {
                if let Some(sender) = self.to_data_processor.get_mut(&subscription.stream_id) {
                    match sender.send(market_data).await {
                        Ok(()) => {}
                        Err(e) => {
                            error!("Failed to send RawMarketData to Data Processor: {e}");
                        }
                    }
                }
            }
        }
    }

    #[allow(clippy::cognitive_complexity)]
    fn handle_router_command(&mut self, command: RouterCommand) {
        match command {
            RouterCommand::Subscribe {
                subscription,
                raw_market_data_sender,
            } => {
                let exchange_stream_id = subscription.exchange_stream_id.clone();
                let stream_id = subscription.stream_id.clone();

                match self.registered_streams.entry(exchange_stream_id) {
                    std::collections::hash_map::Entry::Vacant(e) => {
                        e.insert(subscription);
                        debug!("Registered subscription: {}", stream_id);
                        self.to_data_processor
                            .insert(stream_id.clone(), raw_market_data_sender);
                        info!("Updated raw market channels to include: {}", stream_id);
                    }
                    std::collections::hash_map::Entry::Occupied(_) => {
                        warn!("Trying to register duplicate stream ID: {}", stream_id);
                    }
                }
            }
            RouterCommand::Remove { subscription } => {
                let exchange_stream_id = subscription.exchange_stream_id;
                if self
                    .registered_streams
                    .remove(&exchange_stream_id)
                    .is_some()
                {
                    debug!("Removed subscription: {}", subscription.stream_id);
                } else {
                    warn!(
                        "Tried to remove non-existent subscription: {}",
                        subscription.stream_id
                    );
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
            debug!("Forwarding subscription management confirmation for: {raw_channel} to Orchestrator from: {}", action.as_str());
            match extract_feed_and_symbol(&raw_channel) {
                Ok((feed_type, exchange_symbol)) => {
                    let message = match action {
                        SubscriptionManagementAction::Subscribe => {
                            RouterMessage::ConfirmSubscribe {
                                ws_actor_id: ws_actor_id.to_string(),
                                exchange_symbol,
                                feed_type,
                            }
                        }
                        SubscriptionManagementAction::Unsubscribe => {
                            RouterMessage::ConfirmUnsubscribe {
                                ws_actor_id: ws_actor_id.to_string(),
                                exchange_symbol,
                                feed_type,
                            }
                        }
                        _ => {
                            error!(
                                "❌ send_subscription_confirm received unsupported action: {:?}",
                                action
                            );
                            continue;
                        }
                    };
                    if let Err(e) = self.to_orch.send(message).await {
                        error!(
                            "❌ Failed to send RouterMessage: {:?}, Error: {:?}",
                            action, e
                        );
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
    let parts: Vec<&str> = raw_channel.split('.').collect();
    if parts.len() < 2 {
        return Err(RouterParseError::UnknownChannel(format!(
            "Invalid channel format: {raw_channel}"
        )));
    }
    let feed_type = RequestedFeed::from_exchange_str(parts[0])
        .ok_or_else(|| RouterParseError::UnknownChannel(parts[0].to_string()))?;
    let exchange_symbol = parts[1].to_string();
    Ok((feed_type, exchange_symbol))
}

fn parse_message(exchange_message: &ExchangeMessage) -> ParseMessageResult<ParsedMessage> {
    use SubscriptionManagementAction::{Subscribe, Test, Unsubscribe, UnsubscribeAll};

    let text = match &exchange_message.message {
        Message::Text(text) => text.to_string(),
        Message::Binary(bin) => String::from_utf8(bin.clone().into())
            .map_err(|_| RouterParseError::InvalidMessageType)?,
        _ => {
            warn!(
                "❌ Received unsupported message type: {:?}",
                exchange_message
            );
            return Err(RouterParseError::InvalidMessageType);
        }
    };

    let parsed_message: serde_json::Value = serde_json::from_str(&text)?;

    // ✅ Handle Market Data messages (have a "params" key)
    if let Some(params) = parsed_message.get("params") {
        let exchange_stream_id = params
            .get("channel")
            .and_then(|v| v.as_str())
            .ok_or_else(|| RouterParseError::MissingField("params.channel".to_string()))?
            .to_string();

        let data = params
            .get("data")
            .cloned()
            .ok_or_else(|| RouterParseError::MissingField("params.data".to_string()))?;

        return Ok(ParsedMessage::MarketData {
            exchange_stream_id,
            data,
        });
    }

    // ✅ Handle subscription management messages
    let id = parsed_message
        .get("id")
        .and_then(serde_json::Value::as_u64)
        .ok_or_else(|| RouterParseError::MissingField("message.id".to_string()))?;

    let result = parsed_message
        .get("result")
        .ok_or_else(|| RouterParseError::MissingField("result".to_string()))?;

    let message = match SubscriptionManagementAction::from_id(id) {
        Some(Subscribe | Unsubscribe) => {
            let channels = result
                .as_array()
                .ok_or_else(|| {
                    RouterParseError::MissingField("result (expected array)".to_string())
                })?
                .iter()
                .filter_map(|v| v.as_str())
                .map(String::from)
                .collect();

            match SubscriptionManagementAction::from_id(id) {
                Some(Subscribe) => ParsedMessage::Subscribe { channels },
                Some(Unsubscribe) => ParsedMessage::Unsubscribe { channels },
                _ => unreachable!(),
            }
        }

        Some(UnsubscribeAll) => {
            let result_str = result
                .as_str()
                .ok_or_else(|| {
                    RouterParseError::MissingField("result (expected string)".to_string())
                })?
                .to_string();
            ParsedMessage::UnsubscribeAll { result: result_str }
        }

        Some(Test) => {
            let version = result
                .get("version")
                .and_then(|v| v.as_str())
                .ok_or_else(|| RouterParseError::MissingField("version".to_string()))?
                .to_string();
            ParsedMessage::Test { version }
        }

        None => {
            warn!("⚠️ Unknown subscription management action ID: {}", id);
            return Err(RouterParseError::InvalidSubscriptionManagementMessage(
                parsed_message,
            ));
        }
    };

    Ok(message)
}
