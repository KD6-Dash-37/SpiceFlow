// server/src/async_actors/orchestrator/orch.rs

// 🌍 Standard library
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

// 📦 External crates
use tokio::sync::mpsc;
use tokio::time::{interval, Duration};
use tracing::{debug, error, info, warn, Instrument};

// 🧠 Internal Crates / Modules
use crate::async_actors::broadcast::BroadcastActor;
use crate::async_actors::deribit::{DeribitOrderBookActor, DeribitRouterActor};
use crate::async_actors::messages::{
    BroadcastActorCommand, BroadcastActorMessage, OrderBookCommand, OrderBookMessage,
    ProcessedMarketData, RawMarketData, RouterCommand, RouterMessage, WebSocketCommand,
};
use crate::async_actors::orchestrator::tasks::{TaskOutcome, Workflow, WorkflowKind};
use crate::async_actors::websocket::{
    BinanceWebSocketActor, DeribitWebSocketActor, WebSocketMessage,
};
use crate::domain::ExchangeSubscription;
use crate::http_api::SubscriptionAction;
use crate::model::{Exchange, RequestedFeed};

// 🧩 Local Module
use super::errors::OrchestratorError;
use super::meta::{BroadcastActorMetadata, OrderBookMetadata, RouterMetadata, WebSocketMetadata};

// TODO used for actor ID's make more robust later
static ACTOR_ID_COUNTER: AtomicUsize = AtomicUsize::new(0);

const MAX_STREAMS_PER_WS_ACTOR: usize = 2;
const WS_MESSAGE_BUFFER_SIZE: usize = 32;
const ROUTER_MESSAGE_BUFFER_SIZE: usize = 32;
const OB_MESSAGE_BUFFER_SIZE: usize = 32;
const OB_MD_RAW_BUFFER_SIZE: usize = 32;
const BROADCAST_MESSAGE_BUFFER_SIZE: usize = 32;
const ZERO_MQ_PORT: u16 = 5556;

pub struct Orchestrator {
    pub websockets: HashMap<String, WebSocketMetadata>,
    pub routers: HashMap<String, RouterMetadata>,
    pub orderbooks: HashMap<String, OrderBookMetadata>,
    pub broadcast_actors: HashMap<String, BroadcastActorMetadata>,
    request_receiver: mpsc::Receiver<SubscriptionAction>,
    from_ws: mpsc::Receiver<WebSocketMessage>,
    ws_message_sender: mpsc::Sender<WebSocketMessage>,
    from_router: mpsc::Receiver<RouterMessage>,
    router_message_sender: mpsc::Sender<RouterMessage>,
    from_orderbook: mpsc::Receiver<OrderBookMessage>,
    orderbook_message_sender: mpsc::Sender<OrderBookMessage>,
    from_broadcast_actor: mpsc::Receiver<BroadcastActorMessage>,
    broadcast_actor_message_sender: mpsc::Sender<BroadcastActorMessage>,
    workflows: VecDeque<Workflow>,
}

impl Orchestrator {
    #[must_use]
    pub fn new(request_receiver: mpsc::Receiver<SubscriptionAction>) -> Self {
        let (ws_message_sender, from_ws) = mpsc::channel(WS_MESSAGE_BUFFER_SIZE);
        let (router_message_sender, from_router) = mpsc::channel(ROUTER_MESSAGE_BUFFER_SIZE);
        let (orderbook_message_sender, from_orderbook) = mpsc::channel(OB_MESSAGE_BUFFER_SIZE);
        let (broadcast_actor_message_sender, from_broadcast_actor) =
            mpsc::channel(BROADCAST_MESSAGE_BUFFER_SIZE);
        Self {
            websockets: HashMap::new(),
            routers: HashMap::new(),
            orderbooks: HashMap::new(),
            broadcast_actors: HashMap::new(),
            request_receiver,
            from_ws,
            ws_message_sender,
            from_router,
            router_message_sender,
            from_orderbook,
            orderbook_message_sender,
            from_broadcast_actor,
            broadcast_actor_message_sender,
            workflows: VecDeque::new(),
        }
    }

    pub async fn run(mut self) {
        let span = tracing::info_span!("Orchestrator",);
        async move {
            info!("Starting run loop");
            let mut task_check_interval = interval(Duration::from_millis(50));
            loop {
                tokio::select! {
                    Some(request) = self.request_receiver.recv() => {
                        self.handle_request(request);
                    }
                    Some(ws_message) = self.from_ws.recv() => {
                        self.handle_websocket_message(ws_message);
                    }
                    Some(router_message) = self.from_router.recv() => {
                        self.handle_router_message(router_message);
                    }
                    Some(orderbook_message) = self.from_orderbook.recv() => {
                        self.handle_orderbook_message(orderbook_message).await;
                    }
                    Some(broadcast_actor_message) = self.from_broadcast_actor.recv() => {
                        self.handle_broadcast_actor_message(broadcast_actor_message);
                    }
                    _ = task_check_interval.tick() => {
                        self.tick_workflows().await;
                    }
                    else => break,
                }
            }
        }
        .instrument(span)
        .await;
    }

    fn handle_request(&mut self, request: SubscriptionAction) {
        match request {
            SubscriptionAction::Subscribe(subscription) => {
                #[cfg(feature = "dev-ws-only")]
                {
                    info!(
                        "🧪 Dev mode enabled — spawning WebSocketOnlySubscribe workflow for {}",
                        subscription.stream_id
                    );
                    self.workflows.push_back(Workflow::new(
                        subscription,
                        WorkflowKind::WebSocketOnlySubscribe,
                    ));
                    return;
                }
                self.workflows
                    .push_back(Workflow::new(subscription, WorkflowKind::Subscribe));
            }
            SubscriptionAction::Unsubscribe(subscription) => {
                #[cfg(feature = "dev-ws-only")]
                {
                    info!(
                        "🧪 Dev mode enabled — spawning WebSocketOnlyUnsubscribe workflow for {}",
                        subscription.stream_id
                    );
                    self.workflows.push_back(Workflow::new(
                        subscription,
                        WorkflowKind::WebSocketOnlyUnsubscribe,
                    ));
                    return;
                }
                self.workflows
                    .push_back(Workflow::new(subscription, WorkflowKind::Unsubscribe));
            }
        }
    }

    #[allow(clippy::cognitive_complexity)]
    fn handle_websocket_message(&mut self, ws_message: WebSocketMessage) {
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
                if self.websockets.remove(&actor_id).is_some() {
                    warn!("ws_actor: {actor_id} has been disconnected");
                }
            }
            WebSocketMessage::Shutdown { actor_id } => {
                info!("{} reported shut_down complete", actor_id);
                if self.websockets.remove(&actor_id).is_some() {
                    info!("ws_actor: {actor_id} has been removed from actor registry");
                }
            }
            WebSocketMessage::Error { actor_id, error } => {
                error!(?error, "Error on WebSocket: {actor_id}");
                // optional: initiate retry, teardown, or state tracking
            }
        }
    }

    fn handle_router_message(&mut self, router_message: RouterMessage) {
        match router_message {
            RouterMessage::Heartbeat { actor_id } => {
                if let Some(router_meta) = self.routers.get_mut(&actor_id) {
                    router_meta.last_heartbeat = Some(Instant::now());
                    info!("Received heartbeat from actor: {actor_id}");
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
                if self.orderbooks.remove(&actor_id).is_some() {
                    info!(
                        "Shutdown confirmed for OrderBookActor: {} received, removed from metadata",
                        actor_id
                    );
                } else {
                    warn!("Received shutdown confirmation for OrderBookActor {} but it was not found in metadata", actor_id);
                }
            }
            OrderBookMessage::Resubscribe { subscription } => {
                info!(
                    "Received resubscribe request for {}",
                    subscription.stream_id
                );
                if let Err(e) = self.resubscribe_websocket(&subscription).await {
                    error!("Resubsribing WebSocketActor failed {:?}", e);
                };
            }
        }
    }

    fn handle_broadcast_actor_message(&mut self, message: BroadcastActorMessage) {
        match message {
            BroadcastActorMessage::Heartbeat { actor_id } => {
                if let Some(metadata) = self.broadcast_actors.get_mut(&actor_id) {
                    metadata.last_heartbeat = Some(Instant::now());
                    info!("Received heartbeat from actor: {actor_id}");
                } else {
                    warn!(
                        "Received hearbeat from actor not in registry, actor_id: {}",
                        actor_id
                    );
                }
            }
        }
    }

    #[allow(clippy::cognitive_complexity)] // https://github.com/KD6-Dash-37/SpiceFlow/issues/41
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
            .values()
            .find(|sub| sub.exchange_symbol == exchange_symbol && sub.requested_feed == feed_type)
            .cloned();

        match requested_subscription {
            Some(subscription) => {
                let stream_id = subscription.stream_id.clone();
                ws_meta.requested_streams.remove(&subscription.stream_id);
                debug!(
                    "Confirmed Subscribe for: {} from actor: {}",
                    subscription.stream_id, ws_actor_id
                );
                ws_meta
                    .subscribed_streams
                    .insert(stream_id.clone(), subscription.clone());
                debug!(
                    "Subscribe confirmation updated in Router: {} metadata",
                    router_meta.actor_id
                );
                router_meta
                    .subscribed_streams
                    .insert(stream_id, subscription.clone());
            }
            None => {
                warn!(
                    "Subscribe confirmation received for {:?}: {} but no matching request was found in requested_stream for WebSocketActor: {}",
                    feed_type, exchange_symbol, ws_actor_id
                );
            }
        }
    }

    #[allow(clippy::cognitive_complexity)] // https://github.com/KD6-Dash-37/SpiceFlow/issues/41
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
            .values()
            .find(|sub| sub.exchange_symbol == exchange_symbol && sub.requested_feed == feed_type)
            .cloned();

        match stopped_subscription {
            Some(subscription) => {
                let stream_id = subscription.stream_id.clone();
                // ✅ 3️⃣ Remove from `subscribed_streams`
                if ws_meta
                    .subscribed_streams
                    .remove(&subscription.stream_id)
                    .is_none()
                {
                    warn!(
                        "⚠️ Tried to remove subscription {} from WebSocketMetadata for actor {}, but it wasn't found in subscribed_streams",
                        stream_id, ws_actor_id
                    );
                    return;
                }
                debug!(
                    "✅ Confirmed Unsubscribe for {} from actor: {}",
                    subscription.stream_id, ws_actor_id
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

    pub async fn tick_workflows(&mut self) {
        let task = {
            let Some(workflow) = self.workflows.front() else {
                return;
            };
            match workflow.next_task() {
                Some(task) => task,
                None => return,
            }
        };

        let task_display = task.to_string();

        match task.poll(self).await {
            TaskOutcome::Complete => {
                info!("{} Completed", task_display);
                let Some(workflow) = self.workflows.front_mut() else {
                    return;
                };
                workflow.advance();
                if workflow.is_complete() {
                    info!("✅ {} completed.", workflow);
                    self.workflows.pop_front();
                }
            }
            TaskOutcome::Pending => {}
            TaskOutcome::Error(e) => {
                error!("❌ {} Failed: {:?}", task_display, e);
                self.workflows.pop_front();
            }
        }
    }

    // -------------------------------------------------------
    // Router Management
    // -------------------------------------------------------

    /// Creates and starts a new `RouterActor` for the given exchange.
    ///
    /// # Errors
    /// This function currently always returns `Ok`, but may return an error in the future.
    pub fn create_router(&mut self, exchange: &Exchange) -> Result<String, OrchestratorError> {
        #[cfg(feature = "dev-ws-only")]
        {
            let actor_id = generate_actor_id(&format!("{exchange}DummyRouterActor"));
            let (router_sender, _) = mpsc::channel(1);
            let (router_command_sender, _) = mpsc::channel::<RouterCommand>(1);
            let join_handle = tokio::spawn(async move {
                debug!("Spawned no-op DevRouterActor");
            });
            let mut metadata = RouterMetadata::new(
                actor_id.clone(),
                *exchange,
                router_sender,
                router_command_sender,
                join_handle,
            );
            metadata.last_heartbeat = Some(Instant::now());
            self.routers.insert(actor_id.clone(), metadata);
            return Ok(actor_id);
        }
        debug!("Creating RouterActor for exchange: {exchange}");
        let actor_id = generate_actor_id(&format!("{exchange}RouterActor"));
        let (router_sender, router_receiver) = mpsc::channel(32);
        let (router_command_receiver, from_orch) = mpsc::channel(32);
        let to_orch = self.router_message_sender.clone();

        let router_actor =
            DeribitRouterActor::new(actor_id.clone(), router_receiver, to_orch, from_orch);
        let join_handle = tokio::spawn(router_actor.run());
        debug!("Created a new {exchange}RouterActor, actor_id: {actor_id}");
        let router_meta = RouterMetadata::new(
            actor_id.clone(),
            *exchange,
            router_sender,
            router_command_receiver,
            join_handle,
        );
        self.routers.insert(actor_id.clone(), router_meta);
        Ok(actor_id)
    }

    /// Sends a subscription command to the specified `RouterActor` and registers a raw data channel.
    ///
    /// # Errors
    /// Returns an error if the router is not found, the data channel is missing, or the command fails to send
    pub async fn subscribe_router(
        &self,
        router_actor_id: &str,
        subscription: &ExchangeSubscription,
    ) -> Result<(), OrchestratorError> {
        let stream_id = subscription.stream_id.clone();
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
                        stream_id: stream_id.clone(),
                        router_actor_id: router_actor_id.to_string(),
                    })
                }
            };

        debug!(
            "Registering subscription {} with router {}",
            subscription.stream_id, router_actor_id
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
                subscription.stream_id, router.actor_id, e
            );
            return Err(OrchestratorError::RouterActorTimeout {
                actor_id: router.actor_id.clone(),
            });
        }
        debug!(
            "✅ Successfully registered subscription {} with Router {}",
            subscription.stream_id, router.actor_id
        );
        Ok(())
    }

    /// Sends an unsubscribe command to the specified `RouterActor` and updates its metadata.
    ///
    /// # Errors
    /// Returns an error if the actor is not found, the command fails to send, or the subscription is missing.
    pub async fn unsubscribe_router(
        &mut self,
        router_actor_id: &str,
        subscription: &ExchangeSubscription,
    ) -> Result<(), OrchestratorError> {
        info!(
            "Attempting to unsubscribe {} on RouterActor",
            subscription.stream_id
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

        if router_metadata
            .subscribed_streams
            .remove(&subscription.stream_id)
            .is_some()
        {
            info!(
                "Removed {} from RouterActor: {} metadata.subscribed_streams",
                subscription.stream_id, router_actor_id
            );
        } else {
            warn!(
                "Could not find {} in RouterActor: {} metadata.subscribed_streams",
                subscription.stream_id, router_actor_id
            );
            return Err(OrchestratorError::ExistingSubscriptionNotFound {
                stream_id: subscription.stream_id.clone(),
            });
        }
        Ok(())
    }

    #[must_use]
    pub fn find_ready_router_for_exchange(&self, exchange: Exchange) -> Option<&RouterMetadata> {
        self.routers
            .values()
            .find(|router_meta| router_meta.exchange == exchange && router_meta.is_ready())
    }

    #[must_use]
    pub fn find_available_router(&self, exchange: Exchange) -> Option<String> {
        self.routers
            .iter()
            .filter(|(_, metadata)| metadata.exchange == exchange)
            .map(|(key, _)| key.clone())
            .next()
    }

    /// Returns the `RouterActor` ID associated with the given `WebSocketActor`.
    ///
    /// # Errors
    /// Returns an error if the `WebSocketActor` is not found.
    pub fn get_router_from_ws(&self, ws_actor_id: &str) -> Result<String, OrchestratorError> {
        self.websockets.get(ws_actor_id).map_or_else(
            || {
                Err(OrchestratorError::WebSocketNotFound {
                    actor_id: ws_actor_id.to_string(),
                })
            },
            |ws_meta| Ok(ws_meta.router_actor_id.clone()),
        )
    }

    // -------------------------------------------------------
    // WebSocket Management
    // -------------------------------------------------------

    /// Creates and starts a new `WebSocketActor` for the given exchange.
    ///
    /// # Errors
    /// Returns an error if the associated `RouterActor` cannot be found.
    pub fn create_websocket_actor(
        &mut self,
        exchange: &Exchange,
        router_actor_id: &str,
    ) -> Result<String, OrchestratorError> {
        debug!("Creating WebSocketActor for exchange: {exchange}");
        let Some(router) = self.routers.get(router_actor_id) else {
            error!(
                "Could not find RouterActor ID: {router_actor_id} while creating WebSocketActor"
            );
            return Err(OrchestratorError::RouterActorMissing);
        };
        let router_sender = router.router_sender.clone();
        let actor_id = generate_actor_id(&format!("{exchange}WebSocketActor"));
        let (command_sender, command_receiver) = mpsc::channel::<WebSocketCommand>(32);
        let ws_msg_sender = self.ws_message_sender.clone();
        let join_handle = match exchange {
            Exchange::Deribit => {
                let new_ws_actor = DeribitWebSocketActor::new(
                    command_receiver,
                    ws_msg_sender,
                    router_sender,
                    actor_id.clone(),
                );
                tokio::spawn(new_ws_actor.run())
            }

            Exchange::Binance => {
                let new_ws_actor = BinanceWebSocketActor::new(
                    actor_id.clone(),
                    command_receiver,
                    ws_msg_sender,
                    router_sender,
                );
                tokio::spawn(new_ws_actor.run())
            }
        };

        let requested_streams: HashMap<String, ExchangeSubscription> = HashMap::new();
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
            exchange.to_string()
        );
        Ok(actor_id)
    }

    pub fn get_available_websocket(&mut self) -> Option<String> {
        self.websockets
            .iter()
            .find(|(_, ws_meta)| ws_meta.subscribed_streams.len() < MAX_STREAMS_PER_WS_ACTOR)
            .map(|(actor_id, _)| actor_id.clone())
    }

    /// Sends a subscription command to the specified `WebSocketActor`.
    ///
    /// # Errors
    /// Returns an error if the actor is not found or if the subscription command fails to send.
    pub async fn subscribe_websocket(
        &mut self,
        ws_actor_id: &str,
        subscription: ExchangeSubscription,
    ) -> Result<(), OrchestratorError> {
        let stream_id = subscription.stream_id.clone();
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
        ws_meta
            .requested_streams
            .insert(stream_id.clone(), subscription.clone());

        #[cfg(feature = "dev-ws-only")]
        // In dev mode, we’re skipping Router confirmations, so just assume the subscription is active.
        ws_meta
            .subscribed_streams
            .insert(stream_id.clone(), subscription);

        Ok(())
    }

    /// Sends an unsubscribe command to the specified `WebSocketActor`.
    ///
    /// # Errors
    /// Returns an error if the actor is not found or if the unsubscribe command fails to send.
    pub async fn unsubscribe_websocket(
        &mut self,
        ws_actor_id: &str,
        subscription: ExchangeSubscription,
    ) -> Result<(), OrchestratorError> {
        debug!(
            "Attempting to unsubscribe {} on WebSocketActor: {}",
            subscription.stream_id, ws_actor_id
        );

        let Some(ws_meta) = self.websockets.get_mut(ws_actor_id) else {
            error!(
                "WebsocketActor: {ws_actor_id} not found when subscribing {}",
                subscription.stream_id
            );
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

        debug!(
            "Requested WebSocketActor: {ws_actor_id} to subscribe to {}",
            subscription.stream_id
        );

        Ok(())
    }

    async fn resubscribe_websocket(
        &self,
        subscription: &ExchangeSubscription,
    ) -> Result<(), OrchestratorError> {
        let metadata = self
            .websockets
            .values()
            .find(|meta| {
                meta.subscribed_streams
                    .contains_key(&subscription.stream_id)
            })
            .ok_or(OrchestratorError::WebSocketActorMissing)?;
        metadata
            .command_sender
            .send(WebSocketCommand::Resubscribe(subscription.clone()))
            .await
            .map_err(|_| OrchestratorError::WebSocketActorTimeout {
                actor_id: metadata.actor_id.to_string(),
            })?;
        info!("Sent Resubscribe to WebSocketActor: {}", metadata.actor_id);
        Ok(())
    }

    // -------------------------------------------------------
    // OrderBook Management
    // -------------------------------------------------------

    /// Creates and starts a new `OrderBookActor` for the given subscription.
    ///
    /// # Errors
    /// Returns an error if the associated `BroadcastActor` cannot be found.
    pub fn create_orderbook_actor(
        &mut self,
        subscription: &ExchangeSubscription,
        broadcast_actor_id: &str,
    ) -> Result<String, OrchestratorError> {
        info!(
            "Creating OrderBookActor for {}",
            subscription.exchange_stream_id
        );
        let actor_id = generate_actor_id("OrderBookActor");
        let (command_sender, command_receiver) = mpsc::channel::<OrderBookCommand>(32);
        let to_orch = self.orderbook_message_sender.clone();
        let (raw_market_data_sender, raw_market_data_receiver) =
            mpsc::channel::<RawMarketData>(OB_MD_RAW_BUFFER_SIZE);
        let market_data_sender = match self.get_broadcast_market_data_sender(broadcast_actor_id) {
            Ok(sender) => sender,
            Err(e) => {
                error!(
                    "Failed to get market data channel sender for BroadcastActor: {}",
                    broadcast_actor_id
                );
                return Err(e);
            }
        };
        let new_ob_actor = DeribitOrderBookActor::new(
            actor_id.clone(),
            subscription.clone(),
            to_orch,
            command_receiver,
            raw_market_data_receiver,
            market_data_sender,
        );
        let join_handle = tokio::spawn(new_ob_actor.run());
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
        Ok(actor_id)
    }

    /// Sends a shutdown command to the specified `OrderBookActor` for the given subscription.
    ///
    /// # Errors
    /// Returns an error if the actor or subscription is not found, or if the shutdown command fails to send.
    pub async fn teardown_orderbook_actor(
        &mut self,
        orderbook_actor_id: &str,
        subscription: &ExchangeSubscription,
    ) -> Result<(), OrchestratorError> {
        info!(
            "Sending command to teardown OrderBookActor: {} for {}",
            orderbook_actor_id, subscription.stream_id
        );
        let Some(metadata) = self.orderbooks.get_mut(orderbook_actor_id) else {
            return Err(OrchestratorError::OrderBookNotFound {
                actor_id: orderbook_actor_id.to_string(),
            });
        };
        if metadata.subscription.stream_id != subscription.stream_id {
            return Err(OrchestratorError::ExistingSubscriptionNotFound {
                stream_id: subscription.stream_id.clone(),
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

    #[must_use]
    pub fn get_channel_to_data_processing_actor(
        &self,
        subscription: &ExchangeSubscription,
    ) -> Option<mpsc::Sender<RawMarketData>> {
        match subscription.requested_feed {
            RequestedFeed::OrderBook => self
                .orderbooks
                .iter()
                .find(|(_, ob_meta)| ob_meta.subscription.stream_id == subscription.stream_id)
                .map(|(_, ob_meta)| ob_meta.raw_market_data_sender.clone()),
        }
    }

    #[must_use]
    pub fn check_orderbook_teardown_complete(&self, orderbook_actor_id: &str) -> bool {
        self.orderbooks.contains_key(orderbook_actor_id)
    }

    // -------------------------------------------------------
    // BroadcastActor Management
    // -------------------------------------------------------
    pub fn create_broadcast_actor(&mut self) -> String {
        info!("Creating BroadcastActor");
        let actor_id = generate_actor_id("BroadcastActor");
        let (command_sender, command_receiver) =
            mpsc::channel::<BroadcastActorCommand>(BROADCAST_MESSAGE_BUFFER_SIZE);
        let (market_data_sender, market_data_receiver) = mpsc::channel::<ProcessedMarketData>(32);
        let to_orch = self.broadcast_actor_message_sender.clone();
        let new_bc_actor = BroadcastActor::new(
            actor_id.clone(),
            ZERO_MQ_PORT,
            market_data_receiver,
            command_receiver,
            to_orch,
        );
        let join_handle = tokio::spawn(async move {
            new_bc_actor.run().await;
        });
        self.broadcast_actors.insert(
            actor_id.clone(),
            BroadcastActorMetadata::new(
                actor_id.clone(),
                market_data_sender,
                command_sender,
                join_handle,
            ),
        );
        actor_id
    }

    /// Returns a clone of the market data sender for the specified `BroadcastActor`.
    ///
    /// # Errors
    /// Returns an error if the actor ID is not found.
    pub fn get_broadcast_market_data_sender(
        &self,
        broadcast_actor_id: &str,
    ) -> Result<mpsc::Sender<ProcessedMarketData>, OrchestratorError> {
        self.broadcast_actors.get(broadcast_actor_id).map_or_else(
            || {
                Err(OrchestratorError::BroastcastActorNotFound {
                    actor_id: broadcast_actor_id.to_string(),
                })
            },
            |metadata| Ok(metadata.market_data_sender.clone()),
        )
    }

    /// Sends a shutdown command to the specified `BroadcastActor`.
    ///
    /// # Errors
    /// Returns an error if the actor is not found or if the shutdown command fails to send.
    pub async fn teardown_broadcast_actor(
        &mut self,
        broadcast_actor_id: &str,
    ) -> Result<(), OrchestratorError> {
        info!(
            "Sending command to teardown BroastcastActor: {}",
            broadcast_actor_id
        );
        let Some(metadata) = self.broadcast_actors.get_mut(broadcast_actor_id) else {
            return Err(OrchestratorError::BroastcastActorNotFound {
                actor_id: broadcast_actor_id.to_string(),
            });
        };
        metadata
            .command_sender
            .send(BroadcastActorCommand::Shutdown)
            .await
            .map_err(|_| OrchestratorError::BroastcastActorTimeout {
                actor_id: broadcast_actor_id.to_string(),
            })?;
        Ok(())
    }
}

fn generate_actor_id(actor_type: &str) -> String {
    let id = ACTOR_ID_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{actor_type}.{id}")
}
