// server/src/async_actors/orchestrator/tasks.rs

// 🌍 Standard library
use std::fmt;

// 📦 External Crates
use tracing::info;

// 🧠 Internal Crates / Modules
use super::errors::OrchestratorError;
use crate::async_actors::orchestrator::orch::Orchestrator;
use crate::domain::ExchangeSubscription;
use crate::model::Exchange;

pub enum WorkflowKind {
    Subscribe,
    Unsubscribe,
}

impl fmt::Display for WorkflowKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            WorkflowKind::Subscribe => write!(f, "Subscribe"),
            WorkflowKind::Unsubscribe => write!(f, "Unsubscribe"),
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
        write!(
            f,
            "{}Workflow [{}]",
            self.kind,
            self.subscription.stream_id()
        )
    }
}

impl Workflow {
    pub fn new(subscription: ExchangeSubscription, kind: WorkflowKind) -> Self {
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
                    exchange: self.subscription.exchange(),
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
            OrchestratorTask::CreateOrderBook { subscription } => write!(
                f,
                "OrchestratorTask::Unsubscribe {}",
                subscription.stream_id()
            ),
            OrchestratorTask::OrderBookCreated { subscription } => write!(
                f,
                "OrchestratorTask::OrderBookCreated {}",
                subscription.stream_id()
            ),
            OrchestratorTask::CreateRouterIfNeeded { exchange } => {
                write!(f, "OrchestratorTask::CreateRouterIfNeeded {}", exchange)
            }
            OrchestratorTask::CreateBroadcastActorIfNeeded => {
                write!(f, "OrchestratorTask::CreateBroadcastActorIfNeeded")
            }
            OrchestratorTask::SubscribeRouter { subscription } => write!(
                f,
                "OrchestratorTask::SubscribeRouter {}",
                subscription.stream_id()
            ),
            OrchestratorTask::SubscribeWebSocket { subscription } => write!(
                f,
                "OrchestratorTask::SubscribeWebSocket {}",
                subscription.stream_id()
            ),
            OrchestratorTask::WebSocketSubscribed { subscription } => write!(
                f,
                "OrchestratorTask::WebSocketSubscribed {}",
                subscription.stream_id()
            ),
            OrchestratorTask::UnsubscribeWebSocket { subscription } => write!(
                f,
                "OrchestratorTask::UnsubscribeWebSocket {}",
                subscription.stream_id()
            ),
            OrchestratorTask::UnsubscribeRouter { subscription } => write!(
                f,
                "OrchestratorTask::UnsubscribeRouter {}",
                subscription.stream_id()
            ),
            OrchestratorTask::TeardownOrderBook { subscription } => write!(
                f,
                "OrchestratorTask::TeardownOrderBook {}",
                subscription.stream_id()
            ),
        }
    }
}

impl OrchestratorTask {
    pub async fn poll(self, orch: &mut Orchestrator) -> TaskOutcome {
        match self {
            OrchestratorTask::CreateRouterIfNeeded { exchange } => {
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
                match orch.create_router(&exchange).await {
                    Ok(_) => {
                        info!("Creating RouterActor for exchange: {}", exchange);
                        TaskOutcome::Pending
                    }
                    Err(e) => TaskOutcome::Error(e),
                }
            }

            OrchestratorTask::CreateOrderBook { subscription } => {
                let broadcast_actor_id = match orch
                    .broadcast_actors
                    .values()
                    .next()
                    .map(|meta| meta.actor_id.clone())
                {
                    Some(id) => id,
                    None => return TaskOutcome::Error(OrchestratorError::BroadcastActorMissing),
                };

                if let Err(e) = orch
                    .create_orderbook_actor(&subscription, &broadcast_actor_id)
                    .await
                {
                    return TaskOutcome::Error(e);
                }
                TaskOutcome::Complete
            }

            OrchestratorTask::OrderBookCreated { subscription } => {
                match orch
                    .orderbooks
                    .values()
                    .find(|meta| meta.subscription.stream_id() == subscription.stream_id())
                {
                    Some(meta) => {
                        if meta.is_ready() {
                            info!(
                                "✅ Completed OrchestratorTask::CreateOrderBook for {}",
                                subscription.stream_id()
                            );
                            TaskOutcome::Complete
                        } else {
                            TaskOutcome::Pending
                        }
                    }
                    None => TaskOutcome::Error(OrchestratorError::OrderBookActorMissing),
                }
            }

            OrchestratorTask::CreateBroadcastActorIfNeeded => {
                if let Some(meta) = orch.broadcast_actors.values().next() {
                    return if meta.is_ready() {
                        TaskOutcome::Complete
                    } else {
                        TaskOutcome::Pending
                    };
                };
                orch.create_broadcast_actor().await;
                TaskOutcome::Pending
            }

            OrchestratorTask::SubscribeRouter { subscription } => {
                let router_actor_id = match orch
                    .routers
                    .iter()
                    .find(|(_, meta)| meta.exchange == subscription.exchange())
                    .map(|(id, _)| id.clone())
                {
                    Some(id) => id,
                    None => return TaskOutcome::Error(OrchestratorError::RouterActorMissing),
                };
                if let Err(e) = orch.subscribe_router(&router_actor_id, &subscription).await {
                    TaskOutcome::Error(e)
                } else {
                    TaskOutcome::Complete
                }
            }

            OrchestratorTask::SubscribeWebSocket { subscription } => {
                let ws_actor_id = match orch.get_available_websocket() {
                    Some(ws_id) => ws_id,
                    None => {
                        let router_actor_id = match orch
                            .routers
                            .iter()
                            .find(|(_, meta)| meta.exchange == subscription.exchange())
                            .map(|(id, _)| id.clone())
                        {
                            Some(id) => id,
                            None => {
                                return TaskOutcome::Error(OrchestratorError::RouterActorMissing)
                            }
                        };

                        match orch
                            .create_websocket_actor(&subscription.exchange(), &router_actor_id)
                            .await
                        {
                            Ok(new_id) => new_id,
                            Err(e) => return TaskOutcome::Error(e),
                        }
                    }
                };

                match orch
                    .subscribe_websocket(&ws_actor_id, subscription.clone())
                    .await
                {
                    Ok(_) => TaskOutcome::Complete,
                    Err(e) => TaskOutcome::Error(e),
                }
            }

            OrchestratorTask::WebSocketSubscribed { subscription } => {
                match orch.websockets.iter().find(|(_actor_id, meta)| {
                    meta.subscribed_streams
                        .contains_key(subscription.stream_id())
                }) {
                    Some(_) => TaskOutcome::Complete,
                    None => TaskOutcome::Pending,
                }
            }

            OrchestratorTask::UnsubscribeWebSocket { subscription } => {
                let ws_actor_id = match orch
                    .websockets
                    .values()
                    .find(|meta| {
                        meta.subscribed_streams
                            .contains_key(subscription.stream_id())
                    })
                    .map(|meta| meta.actor_id.clone())
                {
                    Some(id) => id,
                    None => return TaskOutcome::Error(OrchestratorError::WebSocketActorMissing),
                };
                match orch.unsubscribe_websocket(&ws_actor_id, subscription).await {
                    Ok(_) => TaskOutcome::Complete,
                    Err(e) => TaskOutcome::Error(e),
                }
            }

            OrchestratorTask::UnsubscribeRouter { subscription } => {
                let router_actor_id = match orch
                    .routers
                    .values()
                    .find(|meta| {
                        meta.subscribed_streams
                            .contains_key(subscription.stream_id())
                    })
                    .map(|meta| meta.actor_id.clone())
                {
                    Some(id) => id,
                    None => return TaskOutcome::Error(OrchestratorError::RouterActorMissing),
                };
                match orch
                    .unsubscribe_router(&router_actor_id, &subscription)
                    .await
                {
                    Ok(_) => TaskOutcome::Complete,
                    Err(e) => TaskOutcome::Error(e),
                }
            }

            OrchestratorTask::TeardownOrderBook { subscription } => {
                let orderbook_actor_id = match orch
                    .orderbooks
                    .iter()
                    .find(|(_, meta)| meta.subscription.stream_id() == subscription.stream_id())
                    .map(|(key, _)| key.clone())
                {
                    Some(id) => id,
                    None => return TaskOutcome::Error(OrchestratorError::OrderBookActorMissing),
                };
                match orch
                    .teardown_orderbook_actor(&orderbook_actor_id, &subscription)
                    .await
                {
                    Ok(_) => TaskOutcome::Complete,
                    Err(e) => TaskOutcome::Error(e),
                }
            }
        }
    }
}
