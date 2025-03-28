use crate::async_actors::orchestrator::orch::OrchestratorError;
use crate::async_actors::subscription::ExchangeSubscription;
use tokio::sync::mpsc;

#[derive(Debug, Clone)]
pub enum SubscriptionAction {
    Subscribe(ExchangeSubscription),
    Unsubscribe(ExchangeSubscription),
}

#[derive(Clone)]
pub struct OrchestratorHandle {
    sender: mpsc::Sender<SubscriptionAction>,
}

impl OrchestratorHandle {
    pub fn new(sender: mpsc::Sender<SubscriptionAction>) -> Self {
        Self { sender }
    }

    pub async fn send(&self, sub: SubscriptionAction) -> Result<(), OrchestratorError> {
        self.sender
            .send(sub)
            .await
            .map_err(|_| OrchestratorError::ChannelClosed)
    }
}
