use super::actions::{PendingAction, TaskStatus};
use super::common::RequestedFeed;
use super::meta::{OrderBookMetadata, RouterMetadata, WebSocketMetadata};
use crate::async_actors::deribit::orderbook::DeribitOrderBookActor;
use crate::async_actors::deribit::router::DeribitRouterActor;
use crate::async_actors::deribit::websocket::DeribitWebSocketActor;
use crate::async_actors::messages::{
    DummyRequest, Exchange, OrderBookCommand, OrderBookMessage, RawMarketData, RouterCommand,
    RouterMessage, WebSocketCommand, WebSocketMessage,
};
use crate::async_actors::subscription::SubscriptionInfo;
use crate::async_actors::subscription::{DeribitSubscription, ExchangeSubscription};
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;
use thiserror::Error;
use tokio::sync::mpsc;
use tokio::time::{interval, Duration};
use tracing::{debug, error, info, warn, Instrument};
// TODO used for actor ID's make more robust later
static ACTOR_ID_COUNTER: AtomicUsize = AtomicUsize::new(0);

const MAX_STREAMS_PER_WS_ACTOR: usize = 2;
const WS_MESSAGE_BUFFER_SIZE: usize = 32;
const ROUTER_MESSAGE_BUFFER_SIZE: usize = 32;
const OB_MESSAGE_BUFFER_SIZE: usize = 32;
const OB_MD_RAW_BUFFER_SIZE: usize = 32;

#[derive(Debug, Error)]
pub enum OrchestratorError {
    // -------------------------------------------------------
    // Timeout errors
    // -------------------------------------------------------
    #[error("❌ RouterActor {actor_id} timed out")]
    RouterActorTimeout { actor_id: String },
    #[error("❌ WebSocketActor {actor_id} timed out")]
    WebSocketActorTimeout { actor_id: String },
    #[error("❌ OrderBookActor {actor_id} timed out")]
    OrderBookActorTimeout { actor_id: String },

    // -------------------------------------------------------
    // Channel errors
    // -------------------------------------------------------
    #[error("❌ RouterActor {actor_id} ws<>router channel is closed")]
    RouterActorChannelClosed { actor_id: String },
    #[error("Could not find the raw_market_data_sender for {stream_id} while subscribing RouterActor {router_actor_id}")]
    RawDataChannelMissing {
        stream_id: String,
        router_actor_id: String,
    },

    // -------------------------------------------------------
    // Cannot find actor by type
    // -------------------------------------------------------
    #[error("❌ RouterActor not found")]
    RouterActorMissing,
    #[error("❌ WebSocketActor not found")]
    WebSocketActorMissing,
    #[error("❌ OrderBookActor not found")]
    OrderBookActorMissing,

    // -------------------------------------------------------
    // Cannot find actor by ID
    // -------------------------------------------------------
    #[error("❌ WebSocketActor: {actor_id} not found")]
    WebSocketNotFound { actor_id: String },
    #[error("❌ RouterActor: {actor_id} not found")]
    RouterNotFound { actor_id: String },
    #[error("❌ OrderBookActor: {actor_id} not found")]
    OrderBookNotFound { actor_id: String },

    // -------------------------------------------------------
    // Subscription errors
    // -------------------------------------------------------
    #[error("❌ Subscription: {stream_id} not found")]
    ExistingSubscriptionNotFound { stream_id: String },
    #[error("❌ UnsupportedFeedType: {feed_type}")]
    UnsupportedFeedType { feed_type: String },

    #[error("❌ {reason}")]
    TaskLogicError { reason: String },
}

pub struct Orchestrator {
    websockets: HashMap<String, WebSocketMetadata>,
    routers: HashMap<String, RouterMetadata>,
    orderbooks: HashMap<String, OrderBookMetadata>,
    dummy_grpc_receiver: mpsc::Receiver<DummyRequest>,
    from_ws: mpsc::Receiver<WebSocketMessage>,
    ws_message_sender: mpsc::Sender<WebSocketMessage>,
    from_router: mpsc::Receiver<RouterMessage>,
    router_message_sender: mpsc::Sender<RouterMessage>,
    from_orderbook: mpsc::Receiver<OrderBookMessage>,
    orderbook_message_sender: mpsc::Sender<OrderBookMessage>,
    pending_actions: VecDeque<PendingAction>,
}

impl Orchestrator {
    pub fn new(dummy_grpc_receiver: mpsc::Receiver<DummyRequest>) -> Self {
        let (ws_message_sender, from_ws) = mpsc::channel(WS_MESSAGE_BUFFER_SIZE);
        let (router_message_sender, from_router) = mpsc::channel(ROUTER_MESSAGE_BUFFER_SIZE);
        let (orderbook_message_sender, from_orderbook) = mpsc::channel(OB_MESSAGE_BUFFER_SIZE);
        Self {
            websockets: HashMap::new(),
            routers: HashMap::new(),
            orderbooks: HashMap::new(),
            dummy_grpc_receiver,
            from_ws,
            ws_message_sender,
            from_router,
            router_message_sender,
            from_orderbook,
            orderbook_message_sender,
            pending_actions: VecDeque::new(),
        }
    }

    pub async fn run(mut self) {
        let span = tracing::info_span!("Orchestrator",);
        async move {
            info!("Starting run loop");
            let mut task_check_interval = interval(Duration::from_millis(50));
            loop {
                tokio::select! {
                    Some(grpc_request) = self.dummy_grpc_receiver.recv() => {
                        self.handle_request(grpc_request).await;
                    }
                    Some(ws_message) = self.from_ws.recv() => {
                        self.handle_websocket_message(ws_message).await;
                    }
                    Some(router_message) = self.from_router.recv() => {
                        self.handle_router_message(router_message).await;
                    }
                    Some(orderbook_message) = self.from_orderbook.recv() => {
                        self.handle_orderbook_message(orderbook_message).await;
                    }
                    _ = task_check_interval.tick() => {
                        self.process_pending_actions().await;
                    }
                    else => break,
                }
            }
        }
        .instrument(span)
        .await
    }

    async fn handle_request(&mut self, grpc_request: DummyRequest) {
        match grpc_request {
            DummyRequest::Subscribe {
                internal_symbol,
                exchange,
                exchange_symbol,
                requested_feed,
            } => {
                info!(
                    "📡 Received subscribe request for {internal_symbol}.{:?}",
                    requested_feed
                );
                let subscription: ExchangeSubscription = match exchange {
                    Exchange::Deribit => ExchangeSubscription::Deribit(DeribitSubscription::new(
                        internal_symbol,
                        exchange_symbol,
                        requested_feed,
                        Exchange::Deribit,
                    )),
                };
                let subscribe_action = PendingAction::Subscribe {
                    subscription: subscription.clone(),
                    nested_create_orderbook_actor: None,
                    orderbook_actor_id: None,
                    nested_subscribe_router: None,
                    router_actor_id: None,
                    nested_subscribe_websocket: None,
                };
                self.pending_actions.push_back(subscribe_action);
            }

            DummyRequest::Unsubscribe {
                internal_symbol,
                exchange,
                exchange_symbol,
                requested_feed,
            } => {
                info!(
                    "📡 Received unsubscribe request for {internal_symbol}.{:?}",
                    requested_feed
                );
                let subscription = match exchange {
                    Exchange::Deribit => ExchangeSubscription::Deribit(DeribitSubscription::new(
                        internal_symbol,
                        exchange_symbol,
                        requested_feed,
                        Exchange::Deribit,
                    )),
                };
                let unsubscribe_action = PendingAction::Unsubscribe {
                    subscription: subscription.clone(),
                    nested_unsubscribe_ws_actor: None,
                    nested_unsubscribe_router: None,
                    nested_teardown_orderbook: None,
                    ws_unsubscribe_confirmed: false,
                    router_unsubscribe_confirmed: false,
                };
                self.pending_actions.push_back(unsubscribe_action);
            }
        }
    }

    async fn handle_websocket_message(&mut self, ws_message: WebSocketMessage) {
        match ws_message {
            WebSocketMessage::Heartbeat { actor_id } => {
                if let Some(ws_actor_meta) = self.websockets.get_mut(&actor_id) {
                    ws_actor_meta.last_heartbeat = Some(Instant::now());
                    debug!("Received heartbeat from actor: {}", actor_id);
                } else {
                    warn!("Received heartbeat from actor not in registry, actor_id: {actor_id}");
                }
            }
            WebSocketMessage::Disconnected { actor_id } => {
                // TODO review the re-connect policy, what should we do here, try and re-use the existing actor or spawn a new one?
                warn!("{} reported disconnection", actor_id);
                if let Some(_) = self.websockets.remove(&actor_id) {
                    warn!("ws_actor: {actor_id} has been disconnected")
                }
            }
            WebSocketMessage::Shutdown { actor_id } => {
                info!("{} reported shut_down complete", actor_id);
                if let Some(_) = self.websockets.remove(&actor_id) {
                    info!("ws_actor: {actor_id} has been removed from actor registry");
                }
            }
        }
    }

    async fn handle_router_message(&mut self, router_message: RouterMessage) {
        match router_message {
            RouterMessage::Heartbeat { actor_id } => {
                if let Some(router_meta) = self.routers.get_mut(&actor_id) {
                    router_meta.last_heartbeat = Some(Instant::now());
                    debug!("Received heartbeat from actor: {actor_id}");
                } else {
                    warn!("Received hearbeat from actor not in registry, actor_id: {actor_id}");
                }
            }
            RouterMessage::ConfirmSubscribe {
                ws_actor_id,
                exchange_symbol,
                feed_type,
            } => {
                self.process_subscribe_confirmation(&ws_actor_id, &exchange_symbol, feed_type);
            }
            RouterMessage::ConfirmUnsubscribe {
                ws_actor_id,
                exchange_symbol,
                feed_type,
            } => {
                self.process_unsubscribe_confirmation(&ws_actor_id, &exchange_symbol, feed_type);
            }
        }
    }

    async fn handle_orderbook_message(&mut self, orderbook_message: OrderBookMessage) {
        match orderbook_message {
            OrderBookMessage::Heartbeat { actor_id } => {
                if let Some(ob_meta) = self.orderbooks.get_mut(&actor_id) {
                    ob_meta.last_heartbeat = Some(Instant::now());
                    info!("Received heartbeat from actor: {actor_id}");
                } else {
                    warn!("Received heartbeat from actor not in registry, actor_id: {actor_id}");
                }
            }
            OrderBookMessage::Shutdown { actor_id } => {
                if let Some(_) = self.orderbooks.remove(&actor_id) {
                    info!(
                        "Shutdown confirmed for OrderBookActor: {} received, removed from metadata",
                        actor_id
                    );
                } else {
                    warn!("Received shutdown confirmation for OrderBookActor {} but it was not found in metadata", actor_id);
                }
            }
            OrderBookMessage::Resubscribe { subscription } => {
                info!("Received resubscribe request for {}", subscription.stream_id());
                if let Err(e) = self.resubscribe_websocket(&subscription).await {
                    error!("Resubsribing WebSocketActor failed {:?}", e);
                };
            }
        }
    }

    fn process_subscribe_confirmation(
        &mut self,
        ws_actor_id: &str,
        exchange_symbol: &str,
        feed_type: RequestedFeed,
    ) {
        let Some(ws_meta) = self.websockets.get_mut(ws_actor_id) else {
            warn!("Received Subscribe confirmation for unknown WebSocketActor: {ws_actor_id}");
            return;
        };

        let Some(router_meta) = self.routers.get_mut(&ws_meta.router_actor_id) else {
            warn!("Could not find Router {} defined in WebsocketActor meta data while processing subscription confirmation", ws_meta.router_actor_id);
            return;
        };

        let requested_subscription = ws_meta
            .requested_streams
            .iter()
            .find(|sub| sub.exchange_symbol() == exchange_symbol && sub.feed_type() == feed_type)
            .cloned();

        match requested_subscription {
            Some(subscription) => {
                ws_meta.requested_streams.remove(&subscription);
                debug!(
                    "Confirmed Subscribe for: {} from actor: {}",
                    subscription.stream_id(),
                    ws_actor_id
                );
                ws_meta.subscribed_streams.insert(subscription.clone());
                debug!(
                    "Subscribe confirmation updated in Router: {} metadata",
                    router_meta.actor_id
                );
                router_meta.subscribed_streams.insert(subscription.clone());
            }
            None => {
                warn!(
                    "Subscribe confirmation received for {:?}: {} but no matching request was found in requested_stream for WebSocketActor: {}",
                    feed_type, exchange_symbol, ws_actor_id
                );
            }
        }
    }

    fn process_unsubscribe_confirmation(
        &mut self,
        ws_actor_id: &str,
        exchange_symbol: &str,
        feed_type: RequestedFeed,
    ) {
        // ✅ 1️⃣ Retrieve WebSocket metadata.
        let Some(ws_meta) = self.websockets.get_mut(ws_actor_id) else {
            warn!(" Received Unsubscribe confirmation for unknown WebSocketActor: {ws_actor_id}");
            return;
        };

        // ✅ 2️⃣ Look for the subscription in the websocket metadata
        let stopped_subscription = ws_meta
            .subscribed_streams
            .iter()
            .find(|sub| sub.exchange_symbol() == exchange_symbol && sub.feed_type() == feed_type)
            .cloned();

        match stopped_subscription {
            Some(subscription) => {
                // ✅ 3️⃣ Remove from `subscribed_streams`
                if !ws_meta.subscribed_streams.remove(&subscription) {
                    warn!(
                        "⚠️ Tried to remove subscription {} from WebSocketMetadata for actor {}, but it wasn't found in subscribed_streams",
                        subscription.stream_id(), ws_actor_id
                    );
                }
                debug!(
                    "✅ Confirmed Unsubscribe for {} from actor: {}",
                    subscription.stream_id(),
                    ws_actor_id
                );
            }
            None => {
                warn!(
                    "⚠️ Unsubscribe confirmation received for {:?}: {} but no matching request was found in subscribed_streams for WebSocketActor: {}",
                    feed_type, exchange_symbol, ws_actor_id
                );
            }
        }
    }

    async fn process_pending_actions(&mut self) {
        let mut requeue: Vec<PendingAction> = Vec::new();
        while let Some(mut action) = self.pending_actions.pop_front() {
            let status = action.work_on_task(self).await;
            match status {
                TaskStatus::Ready => {
                    info!("✅ Action completed: {action}");
                }
                TaskStatus::NotReady => requeue.push(action),
                TaskStatus::Error(e) => {
                    error!("Action error: {:?}", e);
                }
            }
        }
        for action in requeue {
            self.pending_actions.push_back(action);
        }
    }

    // -------------------------------------------------------
    // General State Management
    // -------------------------------------------------------

    pub fn check_if_subscribed(&mut self, subscription: &ExchangeSubscription) -> bool {
        match &subscription {
            ExchangeSubscription::Deribit(sub) => &sub.stream_id.clone(),
        };
        if let Some(_) = self
            .websockets
            .iter_mut()
            .find(|(_id, meta)| meta.subscribed_streams.contains(&subscription))
        {
            return true;
        }
        return false;
    }

    // -------------------------------------------------------
    // Router Management
    // -------------------------------------------------------
    pub async fn create_router(
        &mut self,
        exchange: &Exchange,
    ) -> Result<String, OrchestratorError> {
        debug!("Creating RouterActor for exchange: {}", exchange.as_str());
        let actor_id = generate_actor_id("DeribitRouterActor".to_string());
        let (router_sender, router_receiver) = mpsc::channel(32);
        let (router_command_receiver, from_orch) = mpsc::channel(32);
        let to_orch = self.router_message_sender.clone();

        let router_actor =
            DeribitRouterActor::new(actor_id.clone(), router_receiver, to_orch, from_orch);
        let join_handle = tokio::spawn(async move {
            router_actor.run().await;
        });
        debug!("Created a new DeribitRouter, actor_id: {}", actor_id);
        let router_meta = RouterMetadata::new(
            actor_id.clone(),
            exchange.clone(),
            router_sender.clone(),
            router_command_receiver,
            join_handle,
        );
        self.routers.insert(actor_id.clone(), router_meta);
        Ok(actor_id)
    }

    pub async fn subscribe_router(
        &self,
        router_actor_id: &str,
        subscription: &ExchangeSubscription,
    ) -> Result<(), OrchestratorError> {
        let stream_id = subscription.stream_id();
        debug!(
            "Fetching router metadata for: {} to register subscription",
            router_actor_id
        );
        let router = self
            .routers
            .get(router_actor_id)
            .ok_or(OrchestratorError::RouterActorMissing)?;

        let raw_market_data_sender: mpsc::Sender<RawMarketData> =
            match self.get_channel_to_data_processing_actor(subscription) {
                Some(sender) => sender,
                None => {
                    return Err(OrchestratorError::RawDataChannelMissing {
                        stream_id: subscription.stream_id().to_string(),
                        router_actor_id: router_actor_id.to_string(),
                    })
                }
            };

        debug!(
            "Registering subscription {} with router {}",
            stream_id, router_actor_id
        );
        if let Err(e) = router
            .router_command_sender
            .send(RouterCommand::Subscribe {
                subscription: subscription.clone(),
                raw_market_data_sender,
            })
            .await
        {
            error!(
                "❌ Failed to send RouterCommand::Register for {} to Router {}: {}",
                stream_id, router.actor_id, e
            );
            return Err(OrchestratorError::RouterActorTimeout {
                actor_id: router.actor_id.clone(),
            });
        }
        debug!(
            "✅ Successfully registered subscription {} with Router {}",
            stream_id, router.actor_id
        );
        Ok(())
    }

    pub async fn unsubscribe_router(
        &mut self,
        router_actor_id: &str,
        subscription: &ExchangeSubscription,
    ) -> Result<(), OrchestratorError> {
        info!(
            "Attempting to unsubscribe {} on RouterActor",
            subscription.stream_id()
        );

        let Some(router_metadata) = self.routers.get_mut(router_actor_id) else {
            return Err(OrchestratorError::RouterNotFound {
                actor_id: router_actor_id.to_string(),
            });
        };

        router_metadata
            .router_command_sender
            .send(RouterCommand::Remove {
                subscription: subscription.clone(),
            })
            .await
            .map_err(|_| OrchestratorError::RouterActorTimeout {
                actor_id: router_actor_id.to_string(),
            })?;

        if router_metadata.subscribed_streams.remove(&subscription) {
            info!(
                "Removed {} from RouterActor: {} metadata.subscribed_streams",
                subscription.stream_id(),
                router_actor_id
            );
        } else {
            warn!(
                "Could not find {} in RouterActor: {} metadata.subscribed_streams",
                subscription.stream_id(),
                router_actor_id
            );
            return Err(OrchestratorError::ExistingSubscriptionNotFound {
                stream_id: subscription.stream_id().to_string(),
            });
        }
        Ok(())
    }

    pub fn find_ready_router_for_exchange(&self, exchange: Exchange) -> Option<&RouterMetadata> {
        self.routers
            .values()
            .find(|router_meta| router_meta.exchange == exchange && router_meta.is_ready())
    }

    pub fn check_router_ready(&self, router_actor_id: &str) -> Result<bool, OrchestratorError> {
        debug!("Checking if Router: {} is ready", router_actor_id);
        match self.routers.get(router_actor_id) {
            Some(router) => {
                debug!("RouterActor: {router_actor_id} is ready");
                return Ok(router.is_ready());
            }
            None => Err(OrchestratorError::RouterActorMissing),
        }
    }

    pub fn find_available_router(&self, exchange: Exchange) -> Option<String> {
        self.routers
            .iter()
            .filter(|(_, metadata)| metadata.exchange == exchange)
            .map(|(key, _)| key.clone())
            .next()
    }

    pub fn router_is_unsubscribed(
        &self,
        router_actor_id: &str,
        subscription: &ExchangeSubscription,
    ) -> Result<bool, OrchestratorError> {
        if let Some(router_meta) = self.routers.get(router_actor_id) {
            Ok(!router_meta.subscribed_streams.contains(&subscription))
        } else {
            Err(OrchestratorError::RouterNotFound {
                actor_id: router_actor_id.to_string(),
            })
        }
    }

    pub fn get_router_from_ws(&self, ws_actor_id: &str) -> Result<String, OrchestratorError> {
        if let Some(ws_meta) = self.websockets.get(ws_actor_id) {
            Ok(ws_meta.router_actor_id.clone())
        } else {
            Err(OrchestratorError::WebSocketNotFound {
                actor_id: ws_actor_id.to_string(),
            })
        }
    }

    pub fn get_subscribed_router_actor_id(
        &mut self,
        subscription: &ExchangeSubscription,
    ) -> Option<String> {
        self.routers
            .iter()
            .find(|(_, metadata)| metadata.subscribed_streams.contains(subscription))
            .map(|(actor_id, _)| actor_id.clone())
    }

    pub fn get_subscribed_router_metadata(
        &self,
        subscription: &ExchangeSubscription,
    ) -> Result<&RouterMetadata, OrchestratorError> {
        self.routers
            .values()
            .find(|router_meta| router_meta.subscribed_streams.contains(subscription))
            .ok_or(OrchestratorError::RouterActorMissing)
    }

    // -------------------------------------------------------
    // WebSocket Management
    // -------------------------------------------------------
    pub async fn create_websocket_actor(
        &mut self,
        exchange: &Exchange,
        router_actor_id: &str,
    ) -> Result<String, OrchestratorError> {
        debug!(
            "Creating WebSocketActor for exchange: {}",
            exchange.as_str()
        );
        let router = match self.routers.get(router_actor_id) {
            Some(router) => router,
            None => {
                error!(
                    "Could not find RouterActor ID: {} while creating WebSocketActor",
                    router_actor_id
                );
                return Err(OrchestratorError::RouterActorMissing);
            }
        };
        let router_sender = router.router_sender.clone();
        let actor_id = generate_actor_id("DeribitWebSocketActor".to_string());
        let (command_sender, command_receiver) = mpsc::channel::<WebSocketCommand>(32);
        let ws_msg_sender = self.ws_message_sender.clone();

        let new_ws_actor = DeribitWebSocketActor::new(
            command_receiver,
            ws_msg_sender,
            router_sender,
            actor_id.clone(),
        );
        let join_handle = tokio::spawn(async move {
            new_ws_actor.run().await;
        });
        let requested_streams: HashSet<ExchangeSubscription> = HashSet::new();
        self.websockets.insert(
            actor_id.clone(),
            WebSocketMetadata::new(
                actor_id.clone(),
                command_sender,
                router.actor_id.clone(),
                requested_streams,
                join_handle,
            ),
        );
        debug!(
            "WebSocket actor successfully created for {}",
            exchange.as_str()
        );
        return Ok(actor_id);
    }

    pub fn check_websocket_ready(&self, ws_actor_id: &str) -> Result<bool, OrchestratorError> {
        match self.websockets.get(ws_actor_id) {
            Some(ws) => {
                return Ok(ws.is_ready());
            }
            None => Err(OrchestratorError::WebSocketActorMissing),
        }
    }

    pub fn get_available_websocket(&mut self) -> Option<String> {
        self.websockets
            .iter()
            .find(|(_, ws_meta)| ws_meta.subscribed_streams.len() < MAX_STREAMS_PER_WS_ACTOR)
            .map(|(actor_id, _)| actor_id.clone())
    }

    pub async fn subscribe_websocket(
        &mut self,
        ws_actor_id: &str,
        subscription: ExchangeSubscription,
    ) -> Result<(), OrchestratorError> {
        let stream_id = subscription.stream_id();
        debug!(
            "Attempting to subscribe {} on WebSocketActor: {}",
            stream_id, ws_actor_id
        );

        let Some(ws_meta) = self.websockets.get_mut(ws_actor_id) else {
            error!("WebsocketActor: {ws_actor_id} not found when subscribing {stream_id}");
            return Err(OrchestratorError::WebSocketNotFound {
                actor_id: ws_actor_id.to_string(),
            });
        };

        ws_meta
            .command_sender
            .send(WebSocketCommand::Subscribe(subscription.clone()))
            .await
            .map_err(|_| OrchestratorError::WebSocketActorTimeout {
                actor_id: ws_actor_id.to_string(),
            })?;

        debug!("Requested subscription of {stream_id} from WebSocketActor: {ws_actor_id}");
        ws_meta.requested_streams.insert(subscription);
        Ok(())
    }

    pub async fn unsubscribe_websocket(
        &mut self,
        ws_actor_id: &str,
        subscription: ExchangeSubscription,
    ) -> Result<(), OrchestratorError> {
        let stream_id = subscription.stream_id();
        debug!(
            "Attempting to subscribe {} on WebSocketActor: {}",
            stream_id, ws_actor_id
        );

        let Some(ws_meta) = self.websockets.get_mut(ws_actor_id) else {
            error!("WebsocketActor: {ws_actor_id} not found when subscribing {stream_id}");
            return Err(OrchestratorError::WebSocketNotFound {
                actor_id: ws_actor_id.to_string(),
            });
        };

        ws_meta
            .command_sender
            .send(WebSocketCommand::Unsubscribe(subscription.clone()))
            .await
            .map_err(|_| OrchestratorError::WebSocketActorTimeout {
                actor_id: ws_actor_id.to_string(),
            })?;

        debug!("Requested WebSocketActor: {ws_actor_id} to subscribe to {stream_id}");

        Ok(())
    }

    pub fn ws_is_subscribed(
        &self,
        ws_actor_id: &str,
        subscription: &ExchangeSubscription,
    ) -> Result<bool, OrchestratorError> {
        if let Some(ws_meta) = self.websockets.get(ws_actor_id) {
            Ok(ws_meta.subscribed_streams.contains(&subscription))
        } else {
            Err(OrchestratorError::WebSocketNotFound {
                actor_id: ws_actor_id.to_string(),
            })
        }
    }

    pub fn ws_is_unsubscribed(
        &self,
        ws_actor_id: &str,
        subscription: &ExchangeSubscription,
    ) -> Result<bool, OrchestratorError> {
        if let Some(ws_meta) = self.websockets.get(ws_actor_id) {
            Ok(!ws_meta.subscribed_streams.contains(&subscription))
        } else {
            Err(OrchestratorError::WebSocketNotFound {
                actor_id: ws_actor_id.to_string(),
            })
        }
    }

    pub fn get_subscribed_ws_actor_id(
        &self,
        subscription: &ExchangeSubscription,
    ) -> Result<String, OrchestratorError> {
        self.websockets
            .iter()
            .find(|(_actor_id, ws_meta)| ws_meta.subscribed_streams.contains(subscription))
            .map(|(actor_id, _)| actor_id.clone())
            .ok_or(OrchestratorError::WebSocketActorMissing)
    }

    pub fn get_subscribed_websocket_meta(
        &self,
        subscription: &ExchangeSubscription
    ) -> Result<&WebSocketMetadata, OrchestratorError> {
        self.websockets
            .values()
            .find(|metadata| metadata.subscribed_streams.contains(subscription))
            .ok_or(OrchestratorError::WebSocketActorMissing)
    }

    async fn resubscribe_websocket(
        &self,
        subscription: &ExchangeSubscription
    ) -> Result<(), OrchestratorError > {
        let metadata = match self.get_subscribed_websocket_meta(subscription) {
            Ok(meta) => meta,
            Err(e) => return Err(e)
        };
        metadata
            .command_sender
            .send(WebSocketCommand::Resubscribe(subscription.clone()))
            .await
            .map_err(|_| OrchestratorError::WebSocketActorTimeout { actor_id: metadata.actor_id.to_string() })?;
        info!("Sent Resubscribe to WebSocketActor: {}", metadata.actor_id);
        Ok(())
    }

    // -------------------------------------------------------
    // OrderBook Management
    // -------------------------------------------------------
    pub async fn create_orderbook_actor(&mut self, subscription: &ExchangeSubscription) {
        info!(
            "Creating OrderBookActor for {}",
            subscription.exchange_stream_id()
        );
        let actor_id = generate_actor_id("OrderBookActor".to_string());
        let (command_sender, command_receiver) = mpsc::channel::<OrderBookCommand>(32);
        let to_orch = self.orderbook_message_sender.clone();
        let (raw_market_data_sender, raw_market_data_receiver) =
            mpsc::channel::<RawMarketData>(OB_MD_RAW_BUFFER_SIZE);
        let new_ob_actor = DeribitOrderBookActor::new(
            actor_id.clone(),
            subscription.clone(),
            to_orch,
            command_receiver,
            raw_market_data_receiver,
        );
        let join_handle = tokio::spawn(async move {
            new_ob_actor.run().await;
        });
        self.orderbooks.insert(
            actor_id.clone(),
            OrderBookMetadata::new(
                actor_id.clone(),
                subscription.clone(),
                command_sender,
                raw_market_data_sender,
                join_handle,
            ),
        );
    }

    pub async fn teardown_orderbook_actor(
        &mut self,
        orderbook_actor_id: &str,
        subscription: &ExchangeSubscription,
    ) -> Result<(), OrchestratorError> {
        info!(
            "Sending command to teardown OrderBookActor: {} for {}",
            orderbook_actor_id,
            subscription.stream_id()
        );
        let Some(metadata) = self.orderbooks.get_mut(orderbook_actor_id) else {
            return Err(OrchestratorError::OrderBookNotFound {
                actor_id: orderbook_actor_id.to_string(),
            });
        };
        if metadata.subscription != *subscription {
            return Err(OrchestratorError::ExistingSubscriptionNotFound {
                stream_id: subscription.stream_id().to_string(),
            });
        }
        metadata
            .orderbook_command_sender
            .send(OrderBookCommand::Shutdown)
            .await
            .map_err(|_| OrchestratorError::OrderBookActorTimeout {
                actor_id: orderbook_actor_id.to_string(),
            })?;

        Ok(())
    }

    pub fn check_orderbook_ready(
        &self,
        orderbook_actor_id: &str,
    ) -> Result<bool, OrchestratorError> {
        info!("Checking if OrderBook {} is ready", orderbook_actor_id);
        match self.orderbooks.get(orderbook_actor_id) {
            Some(orderbook_meta) => {
                return Ok(orderbook_meta.is_ready());
            }
            None => Err(OrchestratorError::OrderBookNotFound {
                actor_id: orderbook_actor_id.to_string(),
            }),
        }
    }

    pub fn get_orderbook_id(&self, subscription: &ExchangeSubscription) -> Option<String> {
        self.orderbooks
            .iter()
            .find(|(_, metadata)| metadata.subscription.stream_id() == subscription.stream_id())
            .map(|(key, _)| key.clone())
    }

    pub fn get_channel_to_data_processing_actor(
        &self,
        subscription: &ExchangeSubscription,
    ) -> Option<mpsc::Sender<RawMarketData>> {
        match subscription.feed_type() {
            RequestedFeed::OrderBook => self
                .orderbooks
                .iter()
                .find(|(_, ob_meta)| ob_meta.subscription == *subscription)
                .map(|(_, ob_meta)| ob_meta.raw_market_data_sender.clone()),
        }
    }

    pub fn check_orderbook_teardown_complete(&self, orderbook_actor_id: &str) -> bool {
        self.orderbooks.contains_key(orderbook_actor_id)
    }
}

fn generate_actor_id(actor_type: String) -> String {
    let id = ACTOR_ID_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{actor_type}.{id}")
}
