use crate::async_actors::deribit::router::DeribitRouterActor;
use crate::async_actors::subscription::SubscriptionInfo;
use crate::async_actors::subscription::{DeribitSubscription, ExchangeSubscription};
use crate::async_actors::deribit::websocket::DeribitWebSocketActor;
use crate::async_actors::messages::{
    DummyRequest, RouterMessage, RouterCommand, WebSocketCommand, WebSocketMessage,
};
use std::collections::{HashMap, HashSet, VecDeque};
use super::actions::{PendingAction, TaskStatus};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;
use thiserror::Error;
use tokio::sync::mpsc;
use tokio::time::{interval, Duration};
use tracing::{debug, error, info, warn, Instrument};
use super::meta::{WebSocketMetadata, RouterMetadata, Exchange};
use super::common::RequestedFeed;
// TODO used for actor ID's make more robust later
static ACTOR_ID_COUNTER: AtomicUsize = AtomicUsize::new(0); 

const MAX_STREAMS_PER_WS_ACTOR: usize = 2;
const WS_MESSAGE_BUFFER_SIZE: usize = 32;
const ROUTER_MESSAGE_BUFFER_SIZE: usize = 32;


#[derive(Debug, Error)]
pub enum OrchestratorError {
    #[error("❌ RouterActor {actor_id} timed out waiting for heartbeat")]
    RouterActorTimeout { actor_id: String },
    #[error("❌ WebSocketActor {actor_id} timed out waiting for heartbeat")]
    WebSocketActorTimeout { actor_id: String },
    #[error("❌ RouterActor {actor_id} ws<>router channel is closed")]
    RouterActorChannelClosed { actor_id: String },
    #[error("❌ RouterActor not found")]
    RouterActorMissing,
    #[error("❌ RouterActor: {actor_id} not found")]
    RouterNotFound { actor_id: String},
    #[error("❌ WebSocketActor not found")]
    WebSocketActorMissing,
    #[error("❌ WebSocketActor: {actor_id} not found")]
    WebSocketNotFound { actor_id: String},
    #[error("❌ Subscription: {stream_id} not found")]
    ExistingSubscriptionNotFound {stream_id: String}
}

pub struct Orchestrator {
    websockets: HashMap<String, WebSocketMetadata>,
    routers: HashMap<String, RouterMetadata>,
    dummy_grpc_receiver: mpsc::Receiver<DummyRequest>,
    from_ws: mpsc::Receiver<WebSocketMessage>,
    ws_message_sender: mpsc::Sender<WebSocketMessage>,
    from_router: mpsc::Receiver<RouterMessage>,
    router_message_sender: mpsc::Sender<RouterMessage>,
    pending_actions: VecDeque<PendingAction>,
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
                let subscription: ExchangeSubscription = match exchange.as_str() {
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
                let exchange_enum = Exchange::Deribit;
                let subscribe_ws_action = PendingAction::SubscribeWebSocket {
                    exchange: exchange_enum,
                    subscription,
                    nested_subscribe_router: None,
                    nested_ws_creation_action: None,
                    existing_ws_actor_id: None,
                    sent_subscribe_message: false
                };
                self.pending_actions.push_back(subscribe_ws_action);
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
                let unsubscribe_ws_action = PendingAction::UnsubscribeWebSocket { 
                    subscription: subscription,
                    websocket_actor_id: None,
                    router_actor_id: None,
                    sent_unsubscribe_command: false,
                    confirmed_unsubscribe: false,
                };
                self.pending_actions.push_back(unsubscribe_ws_action);
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
                self.process_subscribe_confirmation(&ws_actor_id, &exchange_symbol, feed_type);
            }
            RouterMessage::ConfirmUnsubscribe { ws_actor_id, exchange_symbol, feed_type } => {
                self.process_unsubscribe_confirmation(&ws_actor_id, &exchange_symbol, feed_type);
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
                debug!("Confirmed Subscribe for: {} from actor: {}", subscription.stream_id(), ws_actor_id);
                ws_meta.subscribed_streams.insert(subscription.clone());
                debug!("Subscribe confirmation updated in Router: {} metadata", router_meta.actor_id);
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
                debug!("✅ Confirmed Unsubscribe for {} from actor: {}", subscription.stream_id(), ws_actor_id);
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
                TaskStatus::NotReady => {
                    requeue.push(action)
                }
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

    pub fn check_if_subscribed(
        &mut self,
        subscription: &ExchangeSubscription,
    ) -> bool {
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
        exchange: &Exchange
    ) -> Result<String, OrchestratorError> {
        debug!("Creating RouterActor for exchange: {}", exchange.as_str());
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
        let join_handle = tokio::spawn(async move {
            router_actor.run().await;
        });
        debug!("Created a new DeribitRouter, actor_id: {}",actor_id);
        let router_meta = RouterMetadata::new(
            actor_id.clone(),
            exchange.clone(),
            router_sender.clone(),
            router_command_receiver,
            join_handle
        );
        self.routers.insert(actor_id.clone(), router_meta);
        Ok(actor_id)
    }

    pub async fn register_subscription_with_router(
        &self,
        router_actor_id: &str,
        subscription: &ExchangeSubscription
    ) -> Result<(), OrchestratorError> {
        let stream_id = subscription.stream_id();
        debug!("Fetching router metadata for: {} to register subscription", router_actor_id);
        let router = self.routers.get(router_actor_id)
            .ok_or(OrchestratorError::RouterActorMissing)?;

        debug!("Registering subscription {} with router {}",stream_id, router_actor_id);
        if let Err(e) = router
            .router_command_sender
            .send(RouterCommand::Register {
                subscription: subscription.clone(),
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
        debug!("✅ Successfully registered subscription {} with Router {}", stream_id, router.actor_id);
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
            None => Err(OrchestratorError::RouterActorMissing)
        }
    }

    pub fn router_is_unsubscribed(
        &self,
        router_actor_id: &str,
        subscription: &ExchangeSubscription
    ) -> Result<bool, OrchestratorError> {
        if let Some(router_meta) = self.routers.get(router_actor_id) {
            Ok(!router_meta.subscribed_streams.contains(&subscription))
        } else {
            Err(OrchestratorError::RouterNotFound { actor_id: router_actor_id.to_string() })
        }
    }

    pub fn get_router_from_ws(
        &self,
        ws_actor_id: &str
    ) -> Result<String, OrchestratorError> {
        if let Some(ws_meta) = self.websockets.get(ws_actor_id) {
            Ok(ws_meta.router_actor_id.clone())
        } else {
            Err(OrchestratorError::WebSocketNotFound{ actor_id: ws_actor_id.to_string()})
        }
    }

    // -------------------------------------------------------
    // WebSocket Management
    // -------------------------------------------------------
    pub async fn create_websocket_actor(
        &mut self,
        exchange: &Exchange,
        router_actor_id: &str
    ) -> Result<String, OrchestratorError> {
        debug!("Creating WebSocketActor for exchange: {}", exchange.as_str());
        let router = match self.routers.get(router_actor_id) {
            Some(router) => router,
            None => {
                error!("Could not find RouterActor ID: {} while creating WebSocketActor", router_actor_id);
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
        debug!("WebSocket actor successfully created for {}", exchange.as_str());
        return Ok(actor_id)
    }

    pub fn check_websocket_ready(&self, ws_actor_id: &str) -> Result<bool, OrchestratorError> {
        match self.websockets.get(ws_actor_id) {
            Some(ws) => {
                return Ok(ws.is_ready());
            }
            None => Err(OrchestratorError::WebSocketActorMissing)
        }
    }

    pub fn get_available_websocket(
        &mut self,
    ) -> Option<String> {
        self.websockets
            .iter()
            .find(
                |(_, ws_meta)| ws_meta.subscribed_streams.len() < MAX_STREAMS_PER_WS_ACTOR
            )
            .map(|(actor_id, _)|  actor_id.clone())
    }

    pub async fn subscribe_websocket(
        &mut self,
        ws_actor_id: &str,
        subscription: ExchangeSubscription,
    ) -> Result<(), OrchestratorError> {
        let stream_id = subscription.stream_id();
        debug!("Attempting to subscribe {} on WebSocketActor: {}", stream_id, ws_actor_id);

        let Some(ws_meta) = self.websockets.get_mut(ws_actor_id) else {
            error!("WebsocketActor: {ws_actor_id} not found when subscribing {stream_id}");
            return Err(OrchestratorError::WebSocketNotFound { actor_id: ws_actor_id.to_string() });
        };

        ws_meta
            .command_sender
            .send(WebSocketCommand::Subscribe(subscription.clone()))
            .await
            .map_err(|_| OrchestratorError::WebSocketActorTimeout { actor_id: ws_actor_id.to_string() })?;

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
        debug!("Attempting to subscribe {} on WebSocketActor: {}", stream_id, ws_actor_id);

        let Some(ws_meta) = self.websockets.get_mut(ws_actor_id) else {
            error!("WebsocketActor: {ws_actor_id} not found when subscribing {stream_id}");
            return Err(OrchestratorError::WebSocketNotFound { actor_id: ws_actor_id.to_string() });
        };

        ws_meta
            .command_sender
            .send(WebSocketCommand::Unsubscribe(subscription.clone()))
            .await
            .map_err(|_| OrchestratorError::WebSocketActorTimeout { actor_id: ws_actor_id.to_string() })?;

        debug!("Requested WebSocketActor: {ws_actor_id} to subscribe to {stream_id}");

        Ok(())
    }

    pub fn ws_is_subscribed(
        &self,
        ws_actor_id: &str,
        subscription: &ExchangeSubscription
    ) -> Result<bool, OrchestratorError> {
        if let Some(ws_meta) = self.websockets.get(ws_actor_id) {
            Ok(ws_meta.subscribed_streams.contains(&subscription))
        } else {
            Err(OrchestratorError::WebSocketNotFound{ actor_id: ws_actor_id.to_string()})
        }
    }

    pub fn ws_is_unsubscribed(
        &self,
        ws_actor_id: &str,
        subscription: &ExchangeSubscription
    ) -> Result<bool, OrchestratorError>{
        if let Some(ws_meta) = self.websockets.get(ws_actor_id) {
            Ok(!ws_meta.subscribed_streams.contains(&subscription))
        } else {
            Err(OrchestratorError::WebSocketNotFound{ actor_id: ws_actor_id.to_string()})
        }
    }

    pub fn get_subscribed_ws_actor_id(
        &self,
        subscription: &ExchangeSubscription
    ) -> Result<String, OrchestratorError> {
        self.websockets
            .iter()
            .find(
                |(_actor_id, ws_meta)|
                ws_meta.subscribed_streams.contains(subscription)
            )
            .map(|(actor_id, _)| actor_id.clone())
            .ok_or(OrchestratorError::WebSocketActorMissing)
    }

    
}

fn generate_actor_id(actor_type: String) -> String {
    let id = ACTOR_ID_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{actor_type}.{id}")
}
