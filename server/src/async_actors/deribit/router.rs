use crate::async_actors::messages::{ExchangeMessage, RouterMessage};
use tokio::sync::mpsc;
use tokio::time::{self, Duration};
use tracing::{debug, error, info, Instrument};

const ROUTER_HEARTBEAT_INTERVAL: u64 = 5;

pub struct DeribitRouterActor {
    actor_id: String,
    router_receiver: mpsc::Receiver<ExchangeMessage>,
    to_orch: mpsc::Sender<RouterMessage>,
}

impl DeribitRouterActor {
    pub fn new(
        actor_id: String,
        router_receiver: mpsc::Receiver<ExchangeMessage>,
        to_orch: mpsc::Sender<RouterMessage>,
    ) -> Self {
        Self {
            actor_id,
            router_receiver,
            to_orch,
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
                    Some(_) = self.router_receiver.recv() => {
                        debug!(actor_id = %self.actor_id, "Received message")
                    }


                }
            }
        }
        .instrument(span)
        .await
    }

    async fn send_heartbeat(&mut self) -> bool {
        match self
            .to_orch
            .send(RouterMessage::Heartbeat {
                actor_id: self.actor_id.clone(),
            })
            .await
        {
            Ok(_) => {
                info!(actor_id = %self.actor_id, "Sent hearbeat");
                true
            }
            Err(e) => {
                error!(actor_id = %self.actor_id, "Failed to send heartbeat to Orchestrator: {e}");
                false
            }
        }
    }
}
