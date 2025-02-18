use super::common::RequestedFeed;
use super::deribit::router::DeribitRouterActor;
use super::subscription::SubscriptionInfo;
use crate::async_actors::subscription::{DeribitSubscription, ExchangeSubscription};
use crate::async_actors::deribit::websocket::DeribitWebSocketActor;
use crate::async_actors::messages::{
    DummyRequest, ExchangeMessage, RouterMessage, RouterCommand, WebSocketCommand, WebSocketMessage,
};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;
use thiserror::Error;
use tokio::{
    sync::mpsc,
    time::{timeout, Duration},
};
use tracing::{debug, error, info, warn, Instrument};

static ACTOR_ID_COUNTER: AtomicUsize = AtomicUsize::new(0); // TODO used for actor ID's make more robust later

const MAX_STREAMS_PER_WS_ACTOR: usize = 2;
const CREATE_WEBSOCKET_ACTOR_TIMEOUT: u64 = 3;
const CREATE_ROUTER_ACTOR_TIMEOUT: u64 = 3;
const WS_MESSAGE_BUFFER_SIZE: usize = 32;
const ROUTER_MESSAGE_BUFFER_SIZE: usize = 32;

#[derive(Clone)]
pub struct WebSocketMetadata {
    pub actor_id: String,
    pub command_sender: mpsc::Sender<WebSocketCommand>,
    pub router_actor_id: String,
    pub requested_streams: HashSet<ExchangeSubscription>,
    pub subscribed_streams: HashSet<ExchangeSubscription>,
    pub last_heartbeat: Option<Instant>,
}

impl WebSocketMetadata {
    pub fn new(
        actor_id: String,
        command_sender: mpsc::Sender<WebSocketCommand>,
        router_actor_id: String,
        requested_streams: HashSet<ExchangeSubscription>,
    ) -> Self {
        let subscribed_streams = HashSet::new();
        Self {
            actor_id,
            command_sender,
            router_actor_id,
            requested_streams,
            subscribed_streams,
            last_heartbeat: None,
        }
    }
}

pub struct RouterMetadata {
    actor_id: String,
    router_sender: mpsc::Sender<ExchangeMessage>,
    router_command_sender: mpsc::Sender<RouterCommand>,
    pub last_heartbeat: Option<Instant>,
}

impl RouterMetadata {
    pub fn new(
        actor_id: String,
        router_sender: mpsc::Sender<ExchangeMessage>,
        router_command_sender: mpsc::Sender<RouterCommand>
    ) -> Self {
        Self {
            actor_id,
            router_sender,
            router_command_sender,
            last_heartbeat: None,
        }
    }
}

#[derive(Debug, Error)]
pub enum OrchestratorError {
    #[error("❌ RouterActor {actor_id} timed out waiting for heartbeat")]
    RouterActorTimeout { actor_id: String },
    #[error("❌ WebSocketActor {actor_id} timed out waiting for heartbeat")]
    WebSocketActorTimeout { actor_id: String },
    #[error("❌ RouterActor {actor_id} ws<>router channel is closed")]
    RouterActorChannelClosed { actor_id: String },
    #[error("❌ RouterActor not found")]
    RouterActorMissing
}

pub struct Orchestrator {
    websockets: HashMap<String, WebSocketMetadata>,
    routers: HashMap<String, RouterMetadata>,
    dummy_grpc_receiver: mpsc::Receiver<DummyRequest>,
    from_ws: mpsc::Receiver<WebSocketMessage>,
    ws_message_sender: mpsc::Sender<WebSocketMessage>,
    from_router: mpsc::Receiver<RouterMessage>,
    router_message_sender: mpsc::Sender<RouterMessage>,
}

impl Orchestrator {
    pub fn new(dummy_grpc_receiver: mpsc::Receiver<DummyRequest>) -> Self {
        let (ws_message_sender, from_ws) = mpsc::channel(WS_MESSAGE_BUFFER_SIZE);
        let (router_message_sender, from_router) = mpsc::channel(ROUTER_MESSAGE_BUFFER_SIZE);
        Self {
            websockets: HashMap::new(),
            routers: HashMap::new(),
            dummy_grpc_receiver,
            from_ws,
            ws_message_sender,
            from_router,
            router_message_sender,
        }
    }

    pub async fn run(mut self) {
        let span = tracing::info_span!("Orchestrator",);
        async move {
            info!("Starting run loop");
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
                    "Received subscribe request for {internal_symbol}.{:?}",
                    requested_feed
                );
                let subscription = match exchange.as_str() {
                    "Deribit" => ExchangeSubscription::Deribit(DeribitSubscription::new(
                        internal_symbol,
                        exchange_symbol,
                        requested_feed,
                    )),
                    _ => {
                        warn!("Unsupported exchange: {exchange}");
                        return;
                    }
                };
                if let Err(e) = self.subscribe_ws_actor(subscription).await {
                    error!("❌ Subscription request failed: {:?}", e);
                }
            }
            
            DummyRequest::Unsubscribe {
                internal_symbol,
                exchange,
                exchange_symbol,
                requested_feed,
            } => {
                info!("📡 Received unsubscribe request for {internal_symbol}.{:?}", requested_feed);
                let subscription = match exchange.as_str() {
                    "Deribit" => ExchangeSubscription::Deribit(DeribitSubscription::new(
                        internal_symbol,
                        exchange_symbol,
                        requested_feed,
                    )),
                    _ => {
                        warn!("Unsupported exchange: {exchange}");
                        return;
                    }
                };
                if let Err(e) = self.unsubscribe_ws_actor(subscription).await {
                    error!("❌ Unsubscribe request failed: {:?}", e);
                }
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
                    warn!("Received hearbeat from actor not in registry, actor_id: {actor_id}")
                }
            }
            RouterMessage::ConfirmSubscribe { ws_actor_id, exchange_symbol, feed_type} => {
                self.confirm_subscribe(&ws_actor_id, &exchange_symbol, feed_type);
            }
            RouterMessage::ConfirmUnsubscribe { ws_actor_id, exchange_symbol, feed_type } => {
                self.confirm_unsubscribe(&ws_actor_id, &exchange_symbol, feed_type);
            }
            
        }
    }

    async fn create_deribit_ws_actor(
        &mut self,
        subscription: ExchangeSubscription,
    ) -> Result<(), OrchestratorError> {
        // ✅ 1️⃣ Ensure we have a Router before proceeding.
        let (router_actor_id, router_sender) = match self.get_or_create_router().await {
            Ok(router) => router,
            Err(e) => {
                error!("❌ Failed to assign router for WebSocket actor: {:?}", e);
                return Err(e);
            }
        };

        debug!("🛠️ Creating new DeribitWebSocketActor for subscription: {}", subscription.stream_id());

        // ✅ 2️⃣ Set up communication channels for the new WebSocket actor.
        let (command_sender, command_receiver) = mpsc::channel::<WebSocketCommand>(32);
        let ws_msg_sender = self.ws_message_sender.clone();

        // ✅ 3️⃣ Generate a unique actor ID.
        let actor_id = generate_actor_id("DeribitWebSocketActor".to_string());

        // ✅ 4️⃣ Spawn the WebSocket actor.
        let new_ws_actor = DeribitWebSocketActor::new(
            command_receiver,
            ws_msg_sender,
            router_sender.clone(),
            actor_id.clone(),
        );

        tokio::spawn(async move {
            new_ws_actor.run().await;
        });
        info!(
            "✅ Created DeribitWebSocketActor (ID: {}), assigned to Router ID: {}. Waiting for heartbeat...",
            actor_id, router_actor_id
        );

        // ✅ 5️⃣ Wait for the WebSocket actor to confirm it is alive.
        let heartbeat_timeout = Duration::from_secs(CREATE_WEBSOCKET_ACTOR_TIMEOUT);
        let heartbeat_future = async {
            while let Some(msg) = self.from_ws.recv().await {
                if let WebSocketMessage::Heartbeat { actor_id: id } = msg {
                    if id == actor_id {
                        return Ok(());
                    }
                }
            }
            Err(OrchestratorError::WebSocketActorTimeout {
                actor_id: actor_id.clone(),
            })
        };

        match timeout(heartbeat_timeout, heartbeat_future).await {
            Ok(Ok(())) => {
                debug!("✅ Heartbeat received from WebSocket actor: {}", actor_id);
            }
            Ok(Err(e)) => {
                error!("❌ Error during heartbeat wait: {}, actor_id: {}", e, actor_id);
                return Err(e);
            }
            Err(_) => {
                error!("❌ Timeout waiting for heartbeat from WebSocket actor: {}", actor_id);
                return Err(OrchestratorError::WebSocketActorTimeout { actor_id });
            }
        }

        // ✅ 6️⃣ Register the subscription with the Router before sending the Subscribe command.
        match self.routers.get(&router_actor_id) {
            Some(router) => {
                if let Err(e) = router
                    .router_command_sender
                    .send(RouterCommand::Register {
                        subscription: subscription.clone(),
                    })
                    .await
                {
                    error!(
                        "❌ Failed to send RouterCommand::Register for {} to Router {}: {}",
                        subscription.stream_id(),
                        router.actor_id.clone(),
                        e
                    );
                    return Err(OrchestratorError::RouterActorTimeout {
                        actor_id: router.actor_id.clone(),
                    });
                }
                debug!(
                    "✅ Successfully registered {} with Router {}",
                    subscription.stream_id(),
                    router.actor_id
                );
            }
            None => {
                error!(
                    "❌ No active router found while creating WebSocketActor. Subscription: {}",
                    subscription.stream_id()
                );
                return Err(OrchestratorError::RouterActorMissing);
            }
        }
        
        // ✅ 7️⃣ Send the Subscribe command to the WebSocket actor.
        if let Err(e) = command_sender
            .send(WebSocketCommand::Subscribe(subscription.clone()))
            .await
        {
            error!(
                "Failed to send Subscribe command to new actor: {}, error: {}",
                actor_id, e
            );
            return Err(OrchestratorError::WebSocketActorTimeout { actor_id });
        }

        // ✅ 8️⃣ Register the new WebSocket actor metadata.
        let mut requested_streams: HashSet<ExchangeSubscription> = HashSet::new();
        requested_streams.insert(subscription.clone());

        self.websockets.insert(
            actor_id.clone(),
            WebSocketMetadata::new(
                actor_id.clone(),
                command_sender,
                router_actor_id,
                requested_streams,
            ),
        );

        info!(
            "✅ WebSocket actor {} successfully created and registered for stream {}",
            actor_id, subscription.stream_id()
        );

        Ok(())
    }

    async fn get_or_create_router(
        &mut self,
    ) -> Result<(String, mpsc::Sender<ExchangeMessage>), OrchestratorError> {
        debug!("Checking for existing DeribitRouter actors");

        if let Some((router_id, router_meta)) = self.routers.iter().next() {
            info!("Found existing DeribitRouter actor: {}", router_id);
            if router_meta.router_sender.is_closed() {
                error!("Existing RouterActor: {router_id} found, but channel is closed");
                return Err(OrchestratorError::RouterActorChannelClosed {
                    actor_id: router_id.clone(),
                });
            }
            return Ok((router_id.clone(), router_meta.router_sender.clone()));
        }

        info!("Existing DeribitRouter actor not found, spawning one instead");
        let actor_id = generate_actor_id("DeribitRouterActor".to_string());
        let (router_sender, router_receiver) = mpsc::channel(32);
        let (router_command_receiver, from_orch) = mpsc::channel(32);
        let to_orch = self.router_message_sender.clone();

        let router_actor = DeribitRouterActor::new(
            actor_id.clone(),
            router_receiver,
            to_orch,
            from_orch
        );

        tokio::spawn(async move {
            router_actor.run().await;
        });
        debug!(
            "Created a new DeribitRouter, actor_id: {}, waiting for heartbeat",
            actor_id
        );

        let hearbeat_timeout = Duration::from_secs(CREATE_ROUTER_ACTOR_TIMEOUT);

        let heartbeat_result = timeout(hearbeat_timeout, async {
            while let Some(router_message) = self.from_router.recv().await {
                match router_message {
                    RouterMessage::Heartbeat { actor_id: id } if id == actor_id => return Ok(()),
                    _ => {
                        self.handle_router_message(router_message).await;
                    }
                }
            }
            Err(OrchestratorError::RouterActorTimeout {
                actor_id: actor_id.clone(),
            })
        })
        .await;

        match heartbeat_result {
            Ok(Ok(())) => {
                debug!("Received heartbeat from newly spawned router: {}", actor_id);
                let router_meta = RouterMetadata::new(
                    actor_id.clone(),
                    router_sender.clone(),
                    router_command_receiver,
                );
                self.routers.insert(actor_id.clone(), router_meta);
                info!("Created a new DeribitRouter, actor_id: {}", actor_id);
                Ok((actor_id, router_sender))
            }
            Ok(Err(e)) => {
                error!("Router actor failed during heartbeat: {} - {}", actor_id, e);
                Err(e)
            }
            Err(_) => {
                error!("Timeout waiting for router actor: {}", actor_id);
                Err(OrchestratorError::RouterActorTimeout { actor_id })
            }
        }
    }

    async fn subscribe_ws_actor(
        &mut self,
        subscription: ExchangeSubscription,
    ) -> Result<(), OrchestratorError> {
        let stream_id = match &subscription {
            ExchangeSubscription::Deribit(sub) => &sub.stream_id,
        };

        // ✅ 1️⃣ Look for an existing WebSocket actor that is already subscribed to this stream.
        if let Some((actor_id, _)) = self
            .websockets
            .iter_mut()
            .find(|(_id, meta)| meta.subscribed_streams.contains(&subscription))
        {
            info!("Stream {stream_id} is already active in WebSocket actor {actor_id}, no action needed.");
            return Ok(());
        }

        // ✅ 2️⃣ Look for an existing WebSocket actor with capacity.
        if let Some((actor_id, ws_meta)) = self
            .websockets
            .iter_mut()
            .find(|(_id, ws_meta)| ws_meta.subscribed_streams.len() < MAX_STREAMS_PER_WS_ACTOR)
        {
            info!("Assigning stream {stream_id} to existing WebSocket actor {actor_id}");

            // ✅ 3️⃣ Fetch and register the subscription with the Router.
            if let Some(router) = self.routers.get(&ws_meta.router_actor_id) {
                if let Err(e) = router
                    .router_command_sender
                    .send(RouterCommand::Register { subscription: subscription.clone() })
                    .await
                {
                    error!("Failed to send RouterCommand::Register for {} to Router {}: {}", stream_id, router.actor_id, e);
                    return Err(OrchestratorError::RouterActorTimeout { actor_id: ws_meta.router_actor_id.clone() })
                }
            } else {
                error!("No active router found for WebSocketActor: {actor_id} while subscribing {stream_id}, cannot register in router");
                return Err(OrchestratorError::RouterActorMissing)
            }

            // ✅ 4️⃣ Send the Subscribe command to WebSocket actor.
            if let Err(e) = ws_meta
                .command_sender
                .try_send(WebSocketCommand::Subscribe(subscription.clone()))
            {
                error!(
                    "Failed to send Subscribe command for {stream_id}, to actor {actor_id}: {e}"
                );
                return Err(OrchestratorError::WebSocketActorTimeout {
                    actor_id: actor_id.clone(),
                });
            } else {
                ws_meta.requested_streams.insert(subscription.clone());
                info!("Successfully assigned stream {stream_id} to existing WebSocket actor {actor_id}");
            }

            return Ok(());
        }
        
        // ✅ 5️⃣ No existing WebSocket actor found—create a new one.
        info!("Stream capacity exhausted, spawning a new WebSocket actor for {stream_id}");
        if let Err(e) = self.create_deribit_ws_actor(subscription.clone()).await {
            error!("Failed to create WebSocket actor for {stream_id}: {e}");
            return Err(e);
        }

        Ok(())
    }

    async fn unsubscribe_ws_actor(
        &mut self,
        subscription: ExchangeSubscription,
    ) -> Result<(), OrchestratorError> {
        let stream_id = subscription.stream_id();
        
        let (actor_id, ws_meta) = match self
            .websockets
            .iter_mut()
            .find(|(_, meta)| meta.subscribed_streams.contains(&subscription))
        {
            Some((id, meta)) => (id.clone(), meta),
            None => {
                warn!(
                    "Unsubscribe request received for {} but no matching active subscription found in WebSocketActor registry.",
                    stream_id
                );
                return Ok(());
            }
        };

        info!("Unsubscribing from {stream_id} on WebSocket actor: {actor_id}");

        if let Err(e) = ws_meta
            .command_sender
            .send(WebSocketCommand::Unsubscribe(subscription.clone()))
            .await {
                error!(
                    "Failed to send Unsubscribe request for {} to WebSocket actor: {}. Error: {}",
                    stream_id, actor_id, e
                );
                return Err(OrchestratorError::WebSocketActorTimeout { actor_id: actor_id.clone() });
            } else {
                info!("Successfully unsubscribed from {stream_id} on WebSocket actor: {actor_id}");
            }

        Ok(())
    }

    fn confirm_subscribe(
        &mut self,
        ws_actor_id: &str,
        exchange_symbol: &str,
        feed_type: RequestedFeed,
    ) {
        let Some(ws_meta) = self.websockets.get_mut(ws_actor_id) else {
            warn!("Received Subscribe confirmation for unknown WebSocketActor: {ws_actor_id}");
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
                info!("Confirmed Subscribe for: {} from actor: {}", subscription.stream_id(), ws_actor_id);
                ws_meta.subscribed_streams.insert(subscription);
            }
            None => {
                warn!(
                    "Subscribe confirmation received for {:?}: {} but no matching request was found in requested_stream for WebSocketActor: {}",
                    feed_type, exchange_symbol, ws_actor_id
                );
            }
        }
    }

    fn confirm_unsubscribe(
        &mut self,
        ws_actor_id: &str,
        exchange_symbol: &str,
        feed_type: RequestedFeed
    ) {
        // ✅ 1️⃣ Retrieve WebSocket metadata.
        let Some(ws_meta) = self.websockets.get_mut(ws_actor_id) else {
            warn!(" Received Unsubscribe confirmation for unknown WebSocketActor: {ws_actor_id}");
            return;
        };
        
        // ✅ 2️⃣ Look for the subscription in `subscribed_streams`
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
                info!(
                    "✅ Confirmed Unsubscribe for {} from actor: {}",
                    subscription.stream_id(),
                    ws_actor_id
                );

                // ✅ 4️⃣ Send `RouterCommand::Remove` to the Router to unregister it
                if let Some(router) = self.routers.get(&ws_meta.router_actor_id) {
                    if let Err(e) = router
                        .router_command_sender
                        .try_send(RouterCommand::Remove {
                            subscription: subscription.clone() 
                        })
                    {
                        error!(
                            "❌ Failed to send RouterCommand::Remove for {} to Router {}: {}",
                            subscription.stream_id(),
                            router.actor_id,
                            e
                        );
                    } else {
                        info!(
                            "🗑️ Successfully removed {} from Router {}",
                            subscription.stream_id(),
                            router.actor_id
                        );
                    }
                } else {
                    warn!(
                        "⚠️ Router {} not found while handling Unsubscribe for {}",
                        ws_meta.router_actor_id, subscription.stream_id()
                    );
                }
            }
            None => {
                warn!(
                    "⚠️ Unsubscribe confirmation received for {:?}: {} but no matching request was found in subscribed_streams for WebSocketActor: {}",
                    feed_type, exchange_symbol, ws_actor_id
                );
            }
        }
    }
}

fn generate_actor_id(actor_type: String) -> String {
    let id = ACTOR_ID_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{actor_type}.{id}")
}
