// actions.rs
use crate::async_actors::orchestrator::orch::Orchestrator;
use crate::async_actors::orchestrator::orch::OrchestratorError;
use crate::async_actors::subscription::ExchangeSubscription;
use crate::async_actors::subscription::SubscriptionInfo;
use std::{future::Future, pin::Pin};

pub enum TaskStatus {
    Ready,
    NotReady,
    Error(OrchestratorError),
}
use std::fmt;

#[derive(Clone, Debug)]
pub enum PendingAction {
    Subscribe {
        subscription: ExchangeSubscription,
        nested_create_orderbook_actor: Option<Box<PendingAction>>,
        orderbook_actor_id: Option<String>,
        nested_subscribe_router: Option<Box<PendingAction>>,
        router_actor_id: Option<String>,
        nested_subscribe_websocket: Option<Box<PendingAction>>,
    },
    Unsubscribe {
        subscription: ExchangeSubscription,
        nested_unsubscribe_ws_actor: Option<Box<PendingAction>>,
        nested_unsubscribe_router: Option<Box<PendingAction>>,
        nested_teardown_orderbook: Option<Box<PendingAction>>,
        ws_unsubscribe_confirmed: bool,
        router_unsubscribe_confirmed: bool,
    },
    CreateRouter {
        subscription: ExchangeSubscription,
        router_actor_id: Option<String>,
    },
    CreateWebSocket {
        subscription: ExchangeSubscription,
        websocket_actor_id: Option<String>,
        router_actor_id: Option<String>,
        nested_create_router_action: Option<Box<PendingAction>>,
    },
    CreateOrderBook {
        subscription: ExchangeSubscription,
        orderbook_actor_id: Option<String>,
        started_create: bool,
    },
    SubscribeRouter {
        subscription: ExchangeSubscription,
        nested_create_router: Option<Box<PendingAction>>,
        registered: bool,
    },
    SubscribeWebSocket {
        subscription: ExchangeSubscription,
        nested_ws_creation_action: Option<Box<PendingAction>>,
        ws_actor_id: Option<String>,
        sent_subscribe_message: bool,
    },
    UnsubscribeWebSocket {
        subscription: ExchangeSubscription,
        websocket_actor_id: Option<String>,
        sent_unsubscribe_command: bool,
    },
    UnsubscribeRouter {
        subscription: ExchangeSubscription,
        router_actor_id: Option<String>,
        sent_unsubscribe_command: bool,
    },
    TeardownOrderBook {
        subscription: ExchangeSubscription,
        orderbook_actor_id: Option<String>,
        started_teardown: bool,
    },
}

impl fmt::Display for PendingAction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PendingAction::Subscribe { subscription, .. } => {
                write!(f, "Subscribe {{ stream_id: {} }}", subscription.stream_id())
            }

            PendingAction::Unsubscribe { subscription, .. } => {
                write!(
                    f,
                    "Unsubscribe {{ stream_id: {} }}",
                    subscription.stream_id()
                )
            }

            PendingAction::CreateRouter { subscription, .. } => {
                write!(
                    f,
                    "CreateRouter 🔨 {{ exchange: {} }}",
                    subscription.exchange()
                )
            }

            PendingAction::CreateWebSocket { subscription, .. } => {
                write!(
                    f,
                    "CreateWebSocket 🔨 {{ exchange: {} }}",
                    subscription.exchange()
                )
            }

            PendingAction::CreateOrderBook { subscription, .. } => {
                write!(
                    f,
                    "CreateOrderBook 🔨 {{ exchange: {} }}",
                    subscription.exchange().as_str()
                )
            }

            PendingAction::SubscribeRouter { subscription, .. } => {
                write!(
                    f,
                    "SubscribeRouter 📡 {{ stream_id: {} }}",
                    subscription.stream_id()
                )
            }

            PendingAction::SubscribeWebSocket { subscription, .. } => {
                write!(
                    f,
                    "SubscribeWebSocket 📡 {{ stream_id: {} }}",
                    subscription.stream_id()
                )
            }

            PendingAction::UnsubscribeWebSocket { subscription, .. } => {
                write!(
                    f,
                    "UnsubscribeWebSocket {{ stream_id: {} }}",
                    subscription.stream_id()
                )
            }

            PendingAction::UnsubscribeRouter { subscription, .. } => {
                write!(
                    f,
                    "UnsubscribeRouter {{ stream_id: {} }}",
                    subscription.stream_id()
                )
            }

            PendingAction::TeardownOrderBook { subscription, .. } => {
                write!(
                    f,
                    "TeardownOrderBook {{ stream_id: {} }}",
                    subscription.stream_id()
                )
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
                PendingAction::Subscribe {
                    subscription,
                    nested_create_orderbook_actor,
                    orderbook_actor_id,
                    nested_subscribe_router,
                    router_actor_id,
                    nested_subscribe_websocket,
                } => {
                    if nested_create_orderbook_actor.is_none() {
                        *nested_create_orderbook_actor =
                            Some(Box::new(PendingAction::CreateOrderBook {
                                subscription: subscription.clone(),
                                orderbook_actor_id: None,
                                started_create: false,
                            }));
                        return TaskStatus::NotReady;
                    } else {
                        if let Some(ref mut task) = nested_create_orderbook_actor {
                            match task.work_on_task(orch).await {
                                TaskStatus::Ready => match orch.get_orderbook_id(&subscription) {
                                    Some(ob_id) => *orderbook_actor_id = Some(ob_id),
                                    None => return TaskStatus::Error(
                                        OrchestratorError::TaskLogicError {
                                            reason:
                                                "Subscribe Failed to get a valid OrderBook actor ID"
                                                    .to_string(),
                                        },
                                    ),
                                },
                                TaskStatus::NotReady => return TaskStatus::NotReady,
                                TaskStatus::Error(e) => return TaskStatus::Error(e),
                            }
                        }
                    }

                    if nested_subscribe_router.is_none() {
                        *nested_subscribe_router = Some(Box::new(PendingAction::SubscribeRouter {
                            subscription: subscription.clone(),
                            nested_create_router: None,
                            registered: false,
                        }));
                        return TaskStatus::NotReady;
                    } else {
                        if let Some(ref mut task) = nested_subscribe_router {
                            match task.work_on_task(orch).await {
                                TaskStatus::Ready => {
                                    match orch.get_orderbook_id(&subscription) {
                                        Some(rt_id) => {
                                            *router_actor_id = Some(rt_id)
                                        },
                                        None => {
                                            return TaskStatus::Error(OrchestratorError::TaskLogicError {
                                                reason: "Subscribe failed to get a valid RouterActor actor ID".to_string()
                                            })
                                        }

                                    }
                                }
                                TaskStatus::NotReady => return TaskStatus::NotReady,
                                TaskStatus::Error(e) => return TaskStatus::Error(e),
                            }
                        }
                    }

                    if nested_subscribe_websocket.is_none() {
                        *nested_subscribe_websocket =
                            Some(Box::new(PendingAction::SubscribeWebSocket {
                                subscription: subscription.clone(),
                                nested_ws_creation_action: None,
                                ws_actor_id: None,
                                sent_subscribe_message: false,
                            }));
                        return TaskStatus::NotReady;
                    } else {
                        if let Some(ref mut task) = nested_subscribe_websocket {
                            match task.work_on_task(orch).await {
                                TaskStatus::Ready => return TaskStatus::Ready,
                                TaskStatus::NotReady => return TaskStatus::NotReady,
                                TaskStatus::Error(e) => return TaskStatus::Error(e),
                            }
                        }
                    }
                    TaskStatus::Error(OrchestratorError::TaskLogicError {
                        reason: "Fuck knows".to_string(),
                    })
                }

                PendingAction::Unsubscribe {
                    subscription,
                    nested_unsubscribe_ws_actor,
                    nested_unsubscribe_router,
                    nested_teardown_orderbook,
                    ws_unsubscribe_confirmed,
                    router_unsubscribe_confirmed,
                } => {
                    if nested_unsubscribe_ws_actor.is_none() {
                        *nested_unsubscribe_ws_actor =
                            Some(Box::new(PendingAction::UnsubscribeWebSocket {
                                subscription: subscription.clone(),
                                websocket_actor_id: None,
                                sent_unsubscribe_command: false,
                            }));
                        return TaskStatus::NotReady;
                    } else {
                        if !*ws_unsubscribe_confirmed {
                            if let Some(ref mut task) = nested_unsubscribe_ws_actor {
                                match task.work_on_task(orch).await {
                                    TaskStatus::Ready => {
                                        *ws_unsubscribe_confirmed = true;
                                        return TaskStatus::NotReady;
                                    }
                                    TaskStatus::NotReady => return TaskStatus::NotReady,
                                    TaskStatus::Error(e) => return TaskStatus::Error(e),
                                }
                            }
                        }
                    }

                    if nested_unsubscribe_router.is_none() {
                        *nested_unsubscribe_router =
                            Some(Box::new(PendingAction::UnsubscribeRouter {
                                subscription: subscription.clone(),
                                router_actor_id: None,
                                sent_unsubscribe_command: false,
                            }));
                        return TaskStatus::NotReady;
                    } else {
                        if !*router_unsubscribe_confirmed {
                            if let Some(ref mut task) = nested_unsubscribe_router {
                                match task.work_on_task(orch).await {
                                    TaskStatus::Ready => {
                                        *router_unsubscribe_confirmed = true;
                                        return TaskStatus::NotReady;
                                    }
                                    TaskStatus::NotReady => return TaskStatus::NotReady,
                                    TaskStatus::Error(e) => return TaskStatus::Error(e),
                                }
                            }
                        }
                    }
                    if nested_teardown_orderbook.is_none() {
                        *nested_teardown_orderbook =
                            Some(Box::new(PendingAction::TeardownOrderBook {
                                subscription: subscription.clone(),
                                orderbook_actor_id: None,
                                started_teardown: false,
                            }));
                        return TaskStatus::NotReady;
                    }

                    if let Some(ref mut task) = nested_teardown_orderbook {
                        return task.work_on_task(orch).await;
                    }

                    TaskStatus::Error(OrchestratorError::TaskLogicError {
                        reason: "WTF".to_string(),
                    })
                }

                // -------------------------------------------------------
                // CreateRouter
                // -------------------------------------------------------
                PendingAction::CreateRouter {
                    subscription,
                    router_actor_id,
                } => {
                    if let Some(ref id) = *router_actor_id {
                        match orch.check_router_ready(id) {
                            Ok(true) => TaskStatus::Ready,
                            Ok(false) => TaskStatus::NotReady,
                            Err(e) => TaskStatus::Error(e),
                        }
                    } else {
                        match orch.create_router(&subscription.exchange()).await {
                            Ok(id) => {
                                *router_actor_id = Some(id);
                                TaskStatus::NotReady
                            }
                            Err(e) => TaskStatus::Error(e),
                        }
                    }
                }

                // -------------------------------------------------------
                // CreateWebSocket
                // -------------------------------------------------------
                // TODO temove the nested router creation from this task
                PendingAction::CreateWebSocket {
                    subscription,
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
                                        Err(e) => TaskStatus::Error(e),
                                    }
                                } else {
                                    match orch
                                        .create_websocket_actor(&subscription.exchange(), rt_id)
                                        .await
                                    {
                                        Ok(new_ws_id) => {
                                            *websocket_actor_id = Some(new_ws_id);
                                            TaskStatus::NotReady
                                        }
                                        Err(e) => TaskStatus::Error(e),
                                    }
                                }
                            }
                            Ok(false) => TaskStatus::NotReady,
                            Err(e) => TaskStatus::Error(e),
                        }
                    } else {
                        if let Some(router_meta) =
                            orch.find_ready_router_for_exchange(subscription.exchange())
                        {
                            *router_actor_id = Some(router_meta.actor_id.clone());
                            TaskStatus::NotReady
                        } else {
                            if nested_create_router_action.is_none() {
                                *nested_create_router_action =
                                    Some(Box::new(PendingAction::CreateRouter {
                                        subscription: subscription.clone(),
                                        router_actor_id: None,
                                    }));
                            }
                            if let Some(ref mut create_router_action) = nested_create_router_action
                            {
                                match create_router_action.work_on_task(orch).await {
                                    TaskStatus::Ready => TaskStatus::NotReady,
                                    TaskStatus::NotReady => TaskStatus::NotReady,
                                    TaskStatus::Error(e) => TaskStatus::Error(e),
                                }
                            } else {
                                TaskStatus::Error(OrchestratorError::RouterActorMissing)
                            }
                        }
                    }
                }

                // -------------------------------------------------------
                // CreateOrderBook
                // -------------------------------------------------------
                PendingAction::CreateOrderBook {
                    subscription,
                    orderbook_actor_id,
                    started_create,
                } => {
                    // If we have a actor ID we check if it's ready
                    if let Some(ref id) = *orderbook_actor_id {
                        match orch.check_orderbook_ready(id) {
                            Ok(true) => TaskStatus::Ready,
                            Ok(false) => TaskStatus::NotReady,
                            Err(e) => TaskStatus::Error(e),
                        }
                    } else {
                        // See if there is an existing OrderBook actor that has capacity
                        if let Some(ob_id) = orch.get_orderbook_id(&subscription) {
                            *orderbook_actor_id = Some(ob_id);
                            TaskStatus::NotReady
                        } else {
                            // Otherwise we will trigger the creation of a new OrderBook actor
                            // The creation can only be triggered once
                            if !*started_create {
                                orch.create_orderbook_actor(&subscription).await;
                                *started_create = true;
                                return TaskStatus::NotReady;
                            }
                            TaskStatus::NotReady
                        }
                    }
                }

                // -------------------------------------------------------
                // SubscribeRouter
                // -------------------------------------------------------
                PendingAction::SubscribeRouter {
                    subscription,
                    nested_create_router,
                    registered,
                } => {
                    if *registered {
                        return TaskStatus::Ready;
                    }

                    if let Some(router_meta) =
                        orch.find_ready_router_for_exchange(subscription.exchange())
                    {
                        match orch
                            .subscribe_router(&router_meta.actor_id, subscription)
                            .await
                        {
                            Ok(_) => {
                                *registered = true;
                                TaskStatus::Ready
                            }
                            Err(e) => TaskStatus::Error(e),
                        }
                    } else {
                        if nested_create_router.is_none() {
                            *nested_create_router = Some(Box::new(PendingAction::CreateRouter {
                                subscription: subscription.clone(),
                                router_actor_id: None,
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
                // SubscribeWebSocket
                // -------------------------------------------------------
                PendingAction::SubscribeWebSocket {
                    subscription,
                    nested_ws_creation_action,
                    ws_actor_id,
                    sent_subscribe_message,
                } => {
                    // Don't need to do anything if we are already subscribed
                    if orch.check_if_subscribed(subscription) {
                        return TaskStatus::Ready;
                    }

                    if ws_actor_id.is_none() {
                        match orch.get_available_websocket() {
                            Some(ws_id) => {
                                *ws_actor_id = Some(ws_id);
                            }
                            None => {
                                if nested_ws_creation_action.is_none() {
                                    *nested_ws_creation_action =
                                        Some(Box::new(PendingAction::CreateWebSocket {
                                            subscription: subscription.clone(),
                                            websocket_actor_id: None,
                                            router_actor_id: None,
                                            nested_create_router_action: None,
                                        }));
                                }
                                if let Some(ref mut sub_action) = nested_ws_creation_action {
                                    match sub_action.work_on_task(orch).await {
                                        TaskStatus::Ready => {
                                            match orch.get_available_websocket() {
                                                Some(ws_id) => {
                                                    *ws_actor_id = Some(ws_id);
                                                }
                                                None => return TaskStatus::Error(OrchestratorError::TaskLogicError {
                                                    reason: "Could not find an available WebSocketActor after confirming one was created!".to_string() 
                                                })
                                            }
                                        },
                                        TaskStatus::NotReady => return TaskStatus::NotReady,
                                        TaskStatus::Error(e) => return TaskStatus::Error(e)
                                    }
                                }
                            }
                        }
                    }

                    if !*sent_subscribe_message {
                        match orch
                            .subscribe_websocket(
                                ws_actor_id.as_ref().unwrap(),
                                subscription.clone(),
                            )
                            .await
                        {
                            Ok(_) => {
                                *sent_subscribe_message = true;
                                return TaskStatus::NotReady;
                            }
                            Err(e) => return TaskStatus::Error(e),
                        }
                    }
                    match orch.ws_is_subscribed(ws_actor_id.as_ref().unwrap(), subscription) {
                        Ok(true) => TaskStatus::Ready,
                        Ok(false) => TaskStatus::NotReady,
                        Err(e) => TaskStatus::Error(e),
                    }
                }

                // -------------------------------------------------------
                // UnsubscribeWebSocket
                // -------------------------------------------------------
                PendingAction::UnsubscribeWebSocket {
                    subscription,
                    websocket_actor_id,
                    sent_unsubscribe_command,
                } => {
                    // Ensure we have a WebSocket actor ID.
                    if websocket_actor_id.is_none() {
                        match orch.get_subscribed_ws_actor_id(subscription) {
                            Ok(ws_id) => *websocket_actor_id = Some(ws_id),
                            Err(_) => {
                                return TaskStatus::Error(
                                    OrchestratorError::ExistingSubscriptionNotFound {
                                        stream_id: subscription.stream_id().to_string(),
                                    },
                                )
                            }
                        }
                    }

                    let ws_id = websocket_actor_id.as_ref().unwrap();

                    // If we haven't sent the unsubscribe command yet, do so.
                    if !*sent_unsubscribe_command {
                        match orch
                            .unsubscribe_websocket(ws_id, subscription.clone())
                            .await
                        {
                            Ok(_) => {
                                *sent_unsubscribe_command = true;
                                // Wait for confirmation before moving on.
                                return TaskStatus::NotReady;
                            }
                            Err(e) => return TaskStatus::Error(e),
                        }
                    }

                    match orch.ws_is_unsubscribed(ws_id, subscription) {
                        Ok(true) => return TaskStatus::Ready,
                        Ok(false) => return TaskStatus::NotReady,
                        Err(e) => return TaskStatus::Error(e),
                    }
                }

                // -------------------------------------------------------
                // UnsubscribeRouter
                // -------------------------------------------------------
                PendingAction::UnsubscribeRouter {
                    subscription,
                    router_actor_id,
                    sent_unsubscribe_command,
                } => {
                    if router_actor_id.is_none() {
                        match orch.get_subscribed_router_actor_id(subscription) {
                            Some(rt_id) => *router_actor_id = Some(rt_id),
                            None => {
                                return TaskStatus::Error(
                                    OrchestratorError::ExistingSubscriptionNotFound {
                                        stream_id: subscription.stream_id().to_string(),
                                    },
                                )
                            }
                        }
                    }

                    let rt_id = router_actor_id.as_ref().unwrap();

                    if !*sent_unsubscribe_command {
                        match orch.unsubscribe_router(rt_id, subscription).await {
                            Ok(_) => {
                                *sent_unsubscribe_command = true;
                                return TaskStatus::NotReady;
                            }
                            Err(e) => return TaskStatus::Error(e),
                        }
                    }

                    match orch.router_is_unsubscribed(rt_id, subscription) {
                        Ok(true) => return TaskStatus::Ready,
                        Ok(false) => return TaskStatus::NotReady,
                        Err(e) => return TaskStatus::Error(e),
                    }
                }

                // -------------------------------------------------------
                // TeardownOrderBook
                // -------------------------------------------------------
                PendingAction::TeardownOrderBook {
                    subscription,
                    orderbook_actor_id,
                    started_teardown,
                } => {
                    if orderbook_actor_id.is_none() {
                        match orch.get_orderbook_id(subscription) {
                            Some(ob_id) => *orderbook_actor_id = Some(ob_id),
                            None => {
                                return TaskStatus::Error(
                                    OrchestratorError::ExistingSubscriptionNotFound {
                                        stream_id: subscription.stream_id().to_string(),
                                    },
                                )
                            }
                        }
                    }
                    let ob_id = orderbook_actor_id.as_ref().unwrap();
                    if !*started_teardown {
                        match orch.teardown_orderbook_actor(ob_id, subscription).await {
                            Ok(_) => {
                                *started_teardown = true;
                                return TaskStatus::NotReady;
                            }
                            Err(e) => return TaskStatus::Error(e),
                        }
                    }
                    match orch.check_orderbook_teardown_complete(ob_id) {
                        true => return TaskStatus::Ready,
                        false => return TaskStatus::NotReady,
                    }
                }
            }
        })
    }
}
