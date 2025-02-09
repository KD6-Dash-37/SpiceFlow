use super::deribit::router::DeribitRouterActor;
use crate::async_actors::common::Subscription;
use crate::async_actors::deribit::websocket::DeribitWebSocketActor;
use crate::async_actors::messages::{
    DummyRequest, ExchangeMessage, RouterMessage, WebSocketCommand, WebSocketMessage,
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

pub struct WebSocketMetaData {
    pub actor_id: String,
    pub command_sender: mpsc::Sender<WebSocketCommand>,
    pub router_actor_id: String,
    pub subscribed_streams: HashSet<Subscription>,
    pub last_heartbeat: Option<Instant>,
}

impl WebSocketMetaData {
    pub fn new(
        actor_id: String,
        command_sender: mpsc::Sender<WebSocketCommand>,
        router_actor_id: String,
        subscribed_streams: HashSet<Subscription>,
    ) -> Self {
        Self {
            actor_id,
            command_sender,
            router_actor_id,
            subscribed_streams,
            last_heartbeat: None,
        }
    }
}

pub struct RouterMetaData {
    actor_id: String,
    router_sender: mpsc::Sender<ExchangeMessage>,
    pub last_heartbeat: Option<Instant>,
}

impl RouterMetaData {
    pub fn new(actor_id: String, router_sender: mpsc::Sender<ExchangeMessage>) -> Self {
        Self {
            actor_id,
            router_sender,
            last_heartbeat: None,
        }
    }
}

#[derive(Debug, Error)]
pub enum OrchestratorError {
    #[error("RouterActor {actor_id} timed out waiting for heartbeat")]
    RouterActorTimeout { actor_id: String },
    #[error("WebSocketActor {actor_id} timed out waiting for heartbeat")]
    WebSocketActorTimeout { actor_id: String },
    #[error("RouterActor {actor_id} ws<>router channel is closed")]
    RouterActorChannelClosed { actor_id: String },
}

pub struct Orchestrator {
    websockets: HashMap<String, WebSocketMetaData>,
    routers: HashMap<String, RouterMetaData>,
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
                exchange_symbol,
                requested_feed,
            } => {
                info!(
                    "Received subscribe request for {internal_symbol}.{:?}",
                    requested_feed
                );
                let subscription =
                    Subscription::new(internal_symbol, exchange_symbol, requested_feed);
                self.subscribe_ws_actor(subscription).await;
            }
            DummyRequest::Unsubscribe {
                internal_symbol,
                exchange_symbol,
                requested_feed,
            } => {
                info!(
                    "Received unsubscribe request for {}.{:?}",
                    internal_symbol, requested_feed
                );
                let subscription =
                    Subscription::new(internal_symbol, exchange_symbol, requested_feed);
                self.unsubscribe_ws_actor(subscription).await;
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
                    info!("Received heartbeat from actor: {actor_id}");
                } else {
                    warn!("Received hearbeat from actor not in registry, actor_id: {actor_id}")
                }
            }
        }
    }

    async fn create_deribit_ws_actor(
        &mut self,
        subscription: Subscription,
    ) -> Result<(), OrchestratorError> {
        let (router_actor_id, router_sender) = match self.get_or_create_router().await {
            Ok(router) => router,
            Err(e) => {
                error!("Failed to assign router for WebSocket actor: {:?}", e);
                return Err(e);
            }
        };

        info!("Attempting to create a new DeribitWebSocketActor");

        // Channel for the Orchestrator to send messages to the WebSocket actor
        let (command_sender, command_receiver) = mpsc::channel::<WebSocketCommand>(32);
        // Channel that allows the WebSocket actor to send messages to the Orchestrator
        let ws_msg_sender = self.ws_message_sender.clone();

        // Generate a unique actor ID
        let actor_id = generate_actor_id("DeribitWebSocketActor".to_string());

        // Spawn the new actor
        let new_ws_actor = DeribitWebSocketActor::new(
            command_receiver,
            ws_msg_sender,
            router_sender.clone(),
            actor_id.clone(),
        );

        tokio::spawn(async move {
            new_ws_actor.run().await;
        });
        info!("Created a new DeribitWebSocketActor, assigned to Router ID: {}, waiting for heartbeat before sending subscribe command", router_actor_id);

        // Wait for a heartbeat from the new ws actor, with a timeout.
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
                info!("Received heartbeat from newly spawned actor: {}", actor_id);
            }
            Ok(Err(e)) => {
                error!("Error during heartbeat wait: {}, actor_id: {}", e, actor_id);
                return Err(e);
            }
            Err(_) => {
                error!("Timeout waiting for newly spawned actor: {}", actor_id);
                return Err(OrchestratorError::WebSocketActorTimeout { actor_id });
            }
        }

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

        // Register the new actor metadata
        let mut subscribed_streams: HashSet<Subscription> = HashSet::new();
        subscribed_streams.insert(subscription.clone());

        self.websockets.insert(
            actor_id.clone(),
            WebSocketMetaData::new(
                actor_id,
                command_sender,
                router_actor_id,
                subscribed_streams,
            ),
        );

        Ok(())
    }

    async fn get_or_create_router(
        &mut self,
    ) -> Result<(String, mpsc::Sender<ExchangeMessage>), OrchestratorError> {
        info!("Checking for existing DeribitRouter actors");

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
        let to_orch = self.router_message_sender.clone();

        let router_actor = DeribitRouterActor::new(actor_id.clone(), router_receiver, to_orch);

        tokio::spawn(async move {
            router_actor.run().await;
        });
        info!(
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
                info!("Received heartbeat from newly spawned router: {}", actor_id);
                let router_meta = RouterMetaData::new(actor_id.clone(), router_sender.clone());
                self.routers.insert(actor_id.clone(), router_meta);
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
        subscription: Subscription,
    ) -> Result<(), OrchestratorError> {
        let stream_id = subscription.to_stream_id();

        // Look for an actor that is already subscribed to this stream
        if let Some((actor_id, _)) = self
            .websockets
            .iter_mut()
            .find(|(_id, meta)| meta.subscribed_streams.contains(&subscription))
        {
            info!("Stream {stream_id} is already active in WebSocket actor {actor_id}, no action needed.");
            return Ok(());
        }

        // Look for an actor with capacity that does not already subscribe to this stream
        if let Some((actor_id, meta)) = self
            .websockets
            .iter_mut()
            .find(|(_id, meta)| meta.subscribed_streams.len() < MAX_STREAMS_PER_WS_ACTOR)
        {
            info!("Assigning stream {stream_id} to existing WebSocket actor {actor_id}");

            // Send the Subscribe command
            if let Err(e) = meta
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
                meta.subscribed_streams.insert(subscription);
                info!("Successfully assigned stream {stream_id} to existing WebSocket actor {actor_id}");
            }

            return Ok(());
        }

        info!("Stream capacity exhausted, spawning a new WebSocket actor for {stream_id}");

        if let Err(e) = self.create_deribit_ws_actor(subscription).await {
            error!("Failed to create WebSocket actor for {stream_id}: {e}");
            return Err(e);
        }

        Ok(())
    }

    async fn unsubscribe_ws_actor(
        &mut self,
        subscription: Subscription,
    ) -> Result<(), OrchestratorError> {
        let stream_id = subscription.to_stream_id();
        let stream_to_actor = construct_stream_to_actor_map(&self.websockets);

        if let Some(actor_id) = stream_to_actor.get(&stream_id) {
            if let Some(meta) = self.websockets.get_mut(actor_id) {
                info!("Sending Unsubscribe command for: {stream_id}, to actor: {actor_id}");

                if let Err(e) = meta
                    .command_sender
                    .send(WebSocketCommand::Unsubscribe(subscription.clone()))
                    .await
                {
                    error!("Failed to send Unsubscribe WebSocketCommand for: {stream_id}, to actor: {actor_id}, {e}",);
                    return Err(OrchestratorError::WebSocketActorTimeout {
                        actor_id: actor_id.clone(),
                    });
                } else {
                    meta.subscribed_streams.remove(&subscription);
                    info!("WS actor registry metadata update for {actor_id}, {stream_id} removed from subscrbed_streams")
                }
            } else {
                warn!("No actor metadata found for actor id: {}", actor_id);
                return Err(OrchestratorError::WebSocketActorTimeout {
                    actor_id: actor_id.clone(),
                });
            }
        } else {
            warn!(
                "No WebSocketActor found handling subscription: {}",
                stream_id
            );
        }
        Ok(())
    }
}

fn generate_actor_id(actor_type: String) -> String {
    let id = ACTOR_ID_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{actor_type}.{id}")
}

fn construct_stream_to_actor_map(
    websocket_actors: &HashMap<String, WebSocketMetaData>,
) -> HashMap<String, String> {
    let mut map = HashMap::new();
    for (actor_id, meta) in websocket_actors.iter() {
        for subscription in meta.subscribed_streams.iter() {
            map.insert(subscription.to_stream_id(), actor_id.clone());
        }
    }
    map
}
