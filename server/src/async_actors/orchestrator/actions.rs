// actions.rs
use crate::async_actors::subscription::ExchangeSubscription; 
use super::meta::Exchange;
use crate::async_actors::orchestrator::orch::Orchestrator; 
use crate::async_actors::orchestrator::orch::OrchestratorError; 
use std::{
    future::Future,
    pin::Pin,
};

pub enum TaskStatus {
    Ready,
    NotReady,
    Error(OrchestratorError)
}
use std::fmt;

#[derive(Clone, Debug)]
pub enum PendingAction {
    /// Create a new Router actor for the given exchange.
    CreateRouter {
        exchange: Exchange,
        router_actor_id: Option<String>,
    },
    CreateWebSocket {
        exchange: Exchange,
        websocket_actor_id: Option<String>,
        router_actor_id: Option<String>,
        nested_create_router_action: Option<Box<PendingAction>>,
    },
    SubscribeRouter {
        exchange: Exchange,
        subscription: ExchangeSubscription,
        nested_create_router: Option<Box<PendingAction>>,
        registered: bool,
    },
    SubscribeWebSocket {
        exchange: Exchange,
        subscription: ExchangeSubscription,
        nested_subscribe_router: Option<Box<PendingAction>>,
        nested_ws_creation_action: Option<Box<PendingAction>>,
        existing_ws_actor_id: Option<String>,
        sent_subscribe_message: bool
    },
    UnsubscribeWebSocket {
        subscription: ExchangeSubscription,
        websocket_actor_id: Option<String>,
        router_actor_id: Option<String>,
        sent_unsubscribe_command: bool,
        confirmed_unsubscribe: bool,
    }
}

impl fmt::Display for PendingAction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PendingAction::CreateRouter { exchange, .. } => {
                write!(f, "CreateRouter 🔨 {{ exchange: {} }}", exchange.as_str())
            }
            PendingAction::CreateWebSocket { exchange, .. } => {
                write!(f, "CreateWebSocket 🔨 {{ exchange: {} }}", exchange.as_str())
            }
            PendingAction::SubscribeRouter { subscription, .. } => {
                write!(f, "SubscribeRouter 📡 {{ stream_id: {} }}", subscription.stream_id())
            }
            PendingAction::SubscribeWebSocket { subscription, .. } => {
                write!(f, "SubscribeWebSocket 📡 {{ stream_id: {} }}", subscription.stream_id())
            }
            PendingAction::UnsubscribeWebSocket { subscription, websocket_actor_id, .. } => {
                write!(f, "UnsubscribeWebSocket {{ ws_actor_id: {:?}, stream_id: {} }}", websocket_actor_id, subscription.stream_id())
            }
        }
    }
}

impl PendingAction {

    pub fn work_on_task<'a>(
        &'a mut self,
        orch: &'a mut Orchestrator,
    ) -> Pin<Box<dyn Future<Output = TaskStatus> + Send + 'a>> {
        Box::pin(async move {
            match self {
                // -------------------------------------------------------
                // 1) CreateRouter
                // -------------------------------------------------------
                PendingAction::CreateRouter {
                    exchange,
                    router_actor_id,
                } => {
                    if let Some(ref id) = *router_actor_id {
                        match orch.check_router_ready(id) {
                            Ok(true) => TaskStatus::Ready,
                            Ok(false) => TaskStatus::NotReady,
                            Err(e) => TaskStatus::Error(e),
                        }
                    } else {
                        match orch.create_router(exchange).await {
                            Ok(id) => {
                                *router_actor_id = Some(id);
                                TaskStatus::NotReady
                            }
                            Err(e) => TaskStatus::Error(e)
                        }
                    }
                }

                // -------------------------------------------------------
                // 2) CreateWebSocket
                // -------------------------------------------------------
                PendingAction::CreateWebSocket {
                    exchange,
                    websocket_actor_id,
                    router_actor_id,
                    nested_create_router_action,
                } => {
                    if let Some(ref rt_id) = *router_actor_id {
                        match orch.check_router_ready(rt_id) {
                            Ok(true) => {
                                if let Some(ref ws_id) = *websocket_actor_id {
                                    match orch.check_websocket_ready(ws_id) {
                                        Ok(true) => TaskStatus::Ready,
                                        Ok(false) => TaskStatus::NotReady,
                                        Err(e) => TaskStatus::Error(e)
                                    }
                                } else {
                                    match orch.create_websocket_actor(exchange, rt_id).await {
                                        Ok(new_ws_id) => {
                                            *websocket_actor_id = Some(new_ws_id);
                                            TaskStatus::NotReady
                                        }
                                        Err(e) => TaskStatus::Error(e)
                                    }
                                }
                            },
                            Ok(false) => {
                                TaskStatus::NotReady
                            }
                            Err(e) => TaskStatus::Error(e)
                        }
                    } else {
                        if let Some(router_meta) = orch.find_ready_router_for_exchange(*exchange) {
                            *router_actor_id = Some(router_meta.actor_id.clone());
                            TaskStatus::NotReady
                        } else {
                            if nested_create_router_action.is_none() {
                                *nested_create_router_action = Some(Box::new(PendingAction::CreateRouter {
                                    exchange: *exchange,
                                    router_actor_id: None, 
                                }));
                            }
                            if let Some(ref mut create_router_action) = nested_create_router_action {
                                match create_router_action.work_on_task(orch).await {
                                    TaskStatus::Ready => TaskStatus::NotReady,
                                    TaskStatus::NotReady => TaskStatus::NotReady,
                                    TaskStatus::Error(e) => TaskStatus::Error(e)
                                }
                            } else {
                                TaskStatus::Error(OrchestratorError::RouterActorMissing)
                            }
                        }
                    }
                }

                // -------------------------------------------------------
                // 3) SubscribeRouter
                // -------------------------------------------------------
                PendingAction::SubscribeRouter {
                    exchange,
                    subscription,
                    nested_create_router ,
                    registered,
                } => {
                    if *registered {
                        return TaskStatus::Ready;
                    }
                    
                    if let Some(router_meta) = orch.find_ready_router_for_exchange(*exchange) {
                        match orch
                            .register_subscription_with_router(&router_meta.actor_id, subscription)
                            .await {
                                
                                Ok(_) => {
                                    *registered = true;
                                    TaskStatus::Ready
                                },
                                Err(e) => TaskStatus::Error(e)
                            }
                    } else {
                        if nested_create_router.is_none() {
                            *nested_create_router = Some(Box::new(PendingAction::CreateRouter {
                                exchange: *exchange,
                                router_actor_id: None
                            }));
                        }
                        if let Some(ref mut sub_action) = nested_create_router {
                            match sub_action.work_on_task(orch).await {
                                TaskStatus::Ready => TaskStatus::NotReady,
                                TaskStatus::NotReady => TaskStatus::NotReady,
                                TaskStatus::Error(e) => TaskStatus::Error(e),
                            }
                        } else {
                            TaskStatus::Error(OrchestratorError::RouterActorMissing)
                        }
                    }
                }
                
                // -------------------------------------------------------
                // 4) SubscribeWebSocket
                // -------------------------------------------------------
                PendingAction::SubscribeWebSocket {
                    exchange,
                    subscription,
                    nested_subscribe_router,
                    nested_ws_creation_action,
                    existing_ws_actor_id,
                    sent_subscribe_message
                } => {
                    // 1. If already subscribed, return Ready.
                    if orch.check_if_subscribed(subscription) {
                        return TaskStatus::Ready;
                    }
                    
                    // 2. Ensure we have a SubscribeRouter sub-action.
                    if nested_subscribe_router.is_none() {
                        *nested_subscribe_router = Some(Box::new(PendingAction::SubscribeRouter {
                            exchange: *exchange,
                            subscription: subscription.clone(),
                            nested_create_router: None,
                            registered: false,
                        }));
                    }
                    
                    // 3. Process the nested router subscription action.
                    if let Some(ref mut router_sub_action) = nested_subscribe_router {
                        match router_sub_action.work_on_task(orch).await {
                            TaskStatus::Ready => {
                                
                                // Router subscription is complete.
                                // 4. Check if there's an available WebSocket actor
                                if let Some(ws_id) = orch.get_available_websocket() {
                                    *existing_ws_actor_id = Some(ws_id.clone());
                                    
                                    // 5. If we haven't sent the subscribe message yet, do so.
                                    if !*sent_subscribe_message {
                                        match orch.subscribe_websocket(&ws_id,subscription.clone(),).await {
                                            Ok(_) => {
                                                *sent_subscribe_message = true;
                                                TaskStatus::NotReady
                                            },
                                            Err(e) => TaskStatus::Error(e)
                                        }
                                    } else {
                                        // 6. wait for the subscription is confirmed.
                                        match orch.ws_is_subscribed(&ws_id, subscription) {
                                            Ok(true) => TaskStatus::Ready,
                                            Ok(false) => TaskStatus::NotReady,
                                            Err(e) => TaskStatus::Error(e),
                                        }
                                    }
                                    
                                } else {
                                    // 7. No available WS actor; attempt to create one via a nested CreateWebSocket action.
                                    if nested_ws_creation_action.is_none() {
                                        *nested_ws_creation_action = Some(Box::new(PendingAction::CreateWebSocket {
                                            exchange: *exchange,
                                            websocket_actor_id: None,
                                            router_actor_id: None,
                                            nested_create_router_action: None, 
                                        }));
                                    }
                                    if let Some(ref mut create_ws_action) = nested_ws_creation_action {
                                        match create_ws_action.work_on_task(orch).await {
                                            TaskStatus::Ready => TaskStatus::NotReady,
                                            TaskStatus::NotReady => TaskStatus::NotReady,
                                            TaskStatus::Error(e) => TaskStatus::Error(e)
                                        }
                                    } else {
                                        TaskStatus::Error(OrchestratorError::WebSocketActorMissing)
                                    }
                                }
                            }
                            TaskStatus::NotReady => TaskStatus::NotReady,
                            TaskStatus::Error(e) => TaskStatus::Error(e)
                        }
                    } else {
                        TaskStatus::Error(OrchestratorError::RouterActorMissing)
                    }
                }

                PendingAction::UnsubscribeWebSocket {
                    subscription,
                    websocket_actor_id,
                    router_actor_id,
                    sent_unsubscribe_command,
                    confirmed_unsubscribe,
                } => {

                    // Ensure we have a WebSocket actor ID.
                    if websocket_actor_id.is_none() {
                        match orch.get_subscribed_ws_actor_id(subscription) {
                            Ok(ws_id) => *websocket_actor_id = Some(ws_id),
                            Err(_) => return TaskStatus::Error(
                                OrchestratorError::ExistingSubscriptionNotFound {
                                    stream_id: subscription.stream_id().to_string()
                                }
                            ),
                        }
                    }

                    let ws_id = websocket_actor_id.as_ref().unwrap();
                
                    // If we haven't sent the unsubscribe command yet, do so.
                    if !*sent_unsubscribe_command {
                        match orch.unsubscribe_websocket(ws_id, subscription.clone()).await {
                            Ok(_) => {
                                *sent_unsubscribe_command = true;
                                // Wait for confirmation before moving on.
                                return TaskStatus::NotReady;
                            }
                            Err(e) => return TaskStatus::Error(e),
                        }
                    }
                
                    // If the unsubscribe command has been sent but confirmation not received,
                    // check if the WS actor has indeed removed the subscription.
                    if !*confirmed_unsubscribe {
                        match orch.ws_is_unsubscribed(ws_id, subscription) {
                            Ok(true) => {
                                *confirmed_unsubscribe = true;
                                // Proceed to router check on the next iteration.
                                return TaskStatus::NotReady;
                            }
                            Ok(false) => return TaskStatus::NotReady,
                            Err(e) => return TaskStatus::Error(e),
                        }
                    }
                
                    // Once WS unsubscription is confirmed, ensure the router has removed it.
                    if router_actor_id.is_none() {
                        match orch.get_router_from_ws(ws_id) {
                            Ok(rt_id) => *router_actor_id = Some(rt_id),
                            Err(e) => return TaskStatus::Error(e),
                        }
                    }
                    if let Some(ref rt_id) = *router_actor_id {
                        match orch.router_is_unsubscribed(rt_id, subscription) {
                            Ok(true) => TaskStatus::Ready,
                            Ok(false) => TaskStatus::NotReady,
                            Err(e) => TaskStatus::Error(e),
                        }
                    } else {
                        TaskStatus::NotReady
                    }
                }
                
            }
        })
    }
}