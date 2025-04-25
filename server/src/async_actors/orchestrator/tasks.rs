// server/src/async_actors/orchestrator/tasks.rs

// 🌍 Standard library
use std::fmt;

// 📦 External Crates
use tracing::info;

// 🧠 Internal Crates / Modules
use crate::domain::ExchangeSubscription;
use crate::model::Exchange;

// 🧩 Local Module
use super::errors::OrchestratorError;
use super::orch::Orchestrator;

pub enum WorkflowKind {
    Subscribe,
    Unsubscribe,
    #[cfg(feature = "dev-ws-only")]
    WebSocketOnlySubscribe,
    #[cfg(feature = "dev-ws-only")]
    WebSocketOnlyUnsubscribe,
}

impl fmt::Display for WorkflowKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Subscribe => write!(f, "Subscribe"),
            Self::Unsubscribe => write!(f, "Unsubscribe"),
            #[cfg(feature = "dev-ws-only")]
            Self::WebSocketOnlySubscribe => write!(f, "DEV-WebSocketOnlySubscribe"),
            #[cfg(feature = "dev-ws-only")]
            Self::WebSocketOnlyUnsubscribe => write!(f, "DEV-WebSocketOnlyUnsubscribe"),
        }
    }
}

pub struct Workflow {
    kind: WorkflowKind,
    subscription: ExchangeSubscription,
    step: usize,
}

impl fmt::Display for Workflow {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}Workflow [{}]", self.kind, self.subscription.stream_id)
    }
}

impl Workflow {
    pub const fn new(subscription: ExchangeSubscription, kind: WorkflowKind) -> Self {
        Self {
            subscription,
            kind,
            step: 0,
        }
    }

    pub fn next_task(&self) -> Option<OrchestratorTask> {
        match self.kind {
            WorkflowKind::Subscribe => match self.step {
                0 => Some(OrchestratorTask::CreateBroadcastActorIfNeeded),
                1 => Some(OrchestratorTask::CreateOrderBook {
                    subscription: self.subscription.clone(),
                }),
                2 => Some(OrchestratorTask::OrderBookCreated {
                    subscription: self.subscription.clone(),
                }),
                3 => Some(OrchestratorTask::CreateRouterIfNeeded {
                    exchange: self.subscription.exchange,
                }),
                4 => Some(OrchestratorTask::SubscribeRouter {
                    subscription: self.subscription.clone(),
                }),
                5 => Some(OrchestratorTask::SubscribeWebSocket {
                    subscription: self.subscription.clone(),
                }),
                6 => Some(OrchestratorTask::WebSocketSubscribed {
                    subscription: self.subscription.clone(),
                }),
                _ => None,
            },
            WorkflowKind::Unsubscribe => match self.step {
                0 => Some(OrchestratorTask::UnsubscribeWebSocket {
                    subscription: self.subscription.clone(),
                }),
                1 => Some(OrchestratorTask::UnsubscribeRouter {
                    subscription: self.subscription.clone(),
                }),
                2 => Some(OrchestratorTask::TeardownOrderBook {
                    subscription: self.subscription.clone(),
                }),
                _ => None,
            },
            #[cfg(feature = "dev-ws-only")]
            WorkflowKind::WebSocketOnlySubscribe => match self.step {
                0 => Some(OrchestratorTask::CreateRouterIfNeeded {
                    exchange: self.subscription.exchange,
                }),
                1 => Some(OrchestratorTask::SubscribeWebSocket {
                    subscription: self.subscription.clone(),
                }),
                _ => None,
            },
            #[cfg(feature = "dev-ws-only")]
            WorkflowKind::WebSocketOnlyUnsubscribe => match self.step {
                0 => Some(OrchestratorTask::UnsubscribeWebSocket {
                    subscription: self.subscription.clone(),
                }),
                _ => None,
            },
        }
    }

    pub fn advance(&mut self) {
        self.step += 1;
    }

    pub fn is_complete(&self) -> bool {
        self.next_task().is_none()
    }
}

pub enum TaskOutcome {
    Complete,
    Pending,
    Error(OrchestratorError),
}

#[derive(Clone, Debug)]
pub enum OrchestratorTask {
    CreateOrderBook { subscription: ExchangeSubscription },
    OrderBookCreated { subscription: ExchangeSubscription },
    CreateRouterIfNeeded { exchange: Exchange },
    CreateBroadcastActorIfNeeded,
    SubscribeRouter { subscription: ExchangeSubscription },
    SubscribeWebSocket { subscription: ExchangeSubscription },
    WebSocketSubscribed { subscription: ExchangeSubscription },
    UnsubscribeWebSocket { subscription: ExchangeSubscription },
    UnsubscribeRouter { subscription: ExchangeSubscription },
    TeardownOrderBook { subscription: ExchangeSubscription },
}

impl fmt::Display for OrchestratorTask {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CreateOrderBook { subscription } => write!(
                f,
                "OrchestratorTask::Unsubscribe {}",
                subscription.stream_id
            ),
            Self::OrderBookCreated { subscription } => write!(
                f,
                "OrchestratorTask::OrderBookCreated {}",
                subscription.stream_id
            ),
            Self::CreateRouterIfNeeded { exchange } => {
                write!(f, "OrchestratorTask::CreateRouterIfNeeded {exchange}")
            }
            Self::CreateBroadcastActorIfNeeded => {
                write!(f, "OrchestratorTask::CreateBroadcastActorIfNeeded")
            }
            Self::SubscribeRouter { subscription } => write!(
                f,
                "OrchestratorTask::SubscribeRouter {}",
                subscription.stream_id
            ),
            Self::SubscribeWebSocket { subscription } => write!(
                f,
                "OrchestratorTask::SubscribeWebSocket {}",
                subscription.stream_id
            ),
            Self::WebSocketSubscribed { subscription } => write!(
                f,
                "OrchestratorTask::WebSocketSubscribed {}",
                subscription.stream_id
            ),
            Self::UnsubscribeWebSocket { subscription } => write!(
                f,
                "OrchestratorTask::UnsubscribeWebSocket {}",
                subscription.stream_id
            ),
            Self::UnsubscribeRouter { subscription } => write!(
                f,
                "OrchestratorTask::UnsubscribeRouter {}",
                subscription.stream_id
            ),
            Self::TeardownOrderBook { subscription } => write!(
                f,
                "OrchestratorTask::TeardownOrderBook {}",
                subscription.stream_id
            ),
        }
    }
}

#[allow(clippy::too_many_lines)]
impl OrchestratorTask {
    pub async fn poll(self, orch: &mut Orchestrator) -> TaskOutcome {
        match self {
            Self::CreateRouterIfNeeded { exchange } => {
                if let Some(router_meta) = orch
                    .routers
                    .values()
                    .find(|router_meta| router_meta.exchange == exchange)
                {
                    return if router_meta.is_ready() {
                        TaskOutcome::Complete
                    } else {
                        TaskOutcome::Pending
                    };
                }
                match orch.create_router(&exchange) {
                    Ok(_) => {
                        info!("Creating RouterActor for exchange: {}", exchange);
                        TaskOutcome::Pending
                    }
                    Err(e) => TaskOutcome::Error(e),
                }
            }

            Self::CreateOrderBook { subscription } => {
                let Some(broadcast_actor_id) = orch
                    .broadcast_actors
                    .values()
                    .next()
                    .map(|meta| meta.actor_id.clone())
                else {
                    return TaskOutcome::Error(OrchestratorError::BroadcastActorMissing);
                };

                if let Err(e) = orch.create_orderbook_actor(&subscription, &broadcast_actor_id) {
                    return TaskOutcome::Error(e);
                }
                TaskOutcome::Complete
            }

            Self::OrderBookCreated { subscription } => {
                match orch
                    .orderbooks
                    .values()
                    .find(|meta| meta.subscription.stream_id == subscription.stream_id)
                {
                    Some(meta) => {
                        if meta.is_ready() {
                            info!(
                                "✅ Completed OrchestratorTask::CreateOrderBook for {}",
                                subscription.stream_id
                            );
                            TaskOutcome::Complete
                        } else {
                            TaskOutcome::Pending
                        }
                    }
                    None => TaskOutcome::Error(OrchestratorError::OrderBookActorMissing),
                }
            }

            Self::CreateBroadcastActorIfNeeded => {
                if let Some(meta) = orch.broadcast_actors.values().next() {
                    return if meta.is_ready() {
                        TaskOutcome::Complete
                    } else {
                        TaskOutcome::Pending
                    };
                };
                orch.create_broadcast_actor();
                TaskOutcome::Pending
            }

            Self::SubscribeRouter { subscription } => {
                let Some(router_actor_id) = orch
                    .routers
                    .iter()
                    .find(|(_, meta)| meta.exchange == subscription.exchange)
                    .map(|(id, _)| id.clone())
                else {
                    return TaskOutcome::Error(OrchestratorError::RouterActorMissing);
                };
                if let Err(e) = orch.subscribe_router(&router_actor_id, &subscription).await {
                    TaskOutcome::Error(e)
                } else {
                    TaskOutcome::Complete
                }
            }

            Self::SubscribeWebSocket { subscription } => {
                let ws_actor_id = if let Some(ws_id) = orch.get_available_websocket() {
                    ws_id
                } else {
                    let Some(router_actor_id) = orch
                        .routers
                        .iter()
                        .find(|(_, meta)| meta.exchange == subscription.exchange)
                        .map(|(id, _)| id.clone())
                    else {
                        return TaskOutcome::Error(OrchestratorError::RouterActorMissing);
                    };
                    match orch.create_websocket_actor(&subscription.exchange, &router_actor_id) {
                        Ok(new_id) => new_id,
                        Err(e) => return TaskOutcome::Error(e),
                    }
                };

                match orch
                    .subscribe_websocket(&ws_actor_id, subscription.clone())
                    .await
                {
                    Ok(()) => TaskOutcome::Complete,
                    Err(e) => TaskOutcome::Error(e),
                }
            }

            Self::WebSocketSubscribed { subscription } => {
                match orch.websockets.iter().find(|(_actor_id, meta)| {
                    meta.subscribed_streams
                        .contains_key(&subscription.stream_id)
                }) {
                    Some(_) => TaskOutcome::Complete,
                    None => TaskOutcome::Pending,
                }
            }

            Self::UnsubscribeWebSocket { subscription } => {
                let Some(ws_actor_id) = orch
                    .websockets
                    .values()
                    .find(|meta| {
                        meta.subscribed_streams
                            .contains_key(&subscription.stream_id)
                    })
                    .map(|meta| meta.actor_id.clone())
                else {
                    return TaskOutcome::Error(OrchestratorError::RouterActorMissing);
                };

                match orch.unsubscribe_websocket(&ws_actor_id, subscription).await {
                    Ok(()) => TaskOutcome::Complete,
                    Err(e) => TaskOutcome::Error(e),
                }
            }

            Self::UnsubscribeRouter { subscription } => {
                let Some(router_actor_id) = orch
                    .routers
                    .values()
                    .find(|meta| {
                        meta.subscribed_streams
                            .contains_key(&subscription.stream_id)
                    })
                    .map(|meta| meta.actor_id.clone())
                else {
                    return TaskOutcome::Error(OrchestratorError::RouterActorMissing);
                };

                match orch
                    .unsubscribe_router(&router_actor_id, &subscription)
                    .await
                {
                    Ok(()) => TaskOutcome::Complete,
                    Err(e) => TaskOutcome::Error(e),
                }
            }

            Self::TeardownOrderBook { subscription } => {
                let Some(orderbook_actor_id) = orch
                    .orderbooks
                    .iter()
                    .find(|(_, meta)| meta.subscription.stream_id == subscription.stream_id)
                    .map(|(key, _)| key.clone())
                else {
                    return TaskOutcome::Error(OrchestratorError::OrderBookActorMissing);
                };

                match orch
                    .teardown_orderbook_actor(&orderbook_actor_id, &subscription)
                    .await
                {
                    Ok(()) => TaskOutcome::Complete,
                    Err(e) => TaskOutcome::Error(e),
                }
            }
        }
    }
}
