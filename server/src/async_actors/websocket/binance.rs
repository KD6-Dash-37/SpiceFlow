// server/src/async_actors/websocket/binance.rs

// 📦 External Crates
use futures_util::stream::{SplitSink, SplitStream, StreamExt};
use futures_util::SinkExt;
use tokio::sync::mpsc;
use tokio_tungstenite::{
    connect_async,
    tungstenite::protocol::Message,
    MaybeTlsStream, WebSocketStream,
};
use tokio::time::{self, timeout, Duration};
use tracing::{debug, error, info, warn, Instrument};

// 🧠 Internal Crates / Modules
use crate::async_actors::messages::{WebSocketCommand, ExchangeMessage};
use crate::domain::ExchangeSubscription;
use super::{WebSocketActorError, WebSocketMessage};
// 🔧 Type Definitions
type WsStream = WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;

const WEBSOCKET_HEARTBEAT_INTERVAL: u64 = 5;
const BINANCE_SPOT_WS_URL: &str = "wss://stream.binance.com:9443/stream"; 

#[derive(Debug)]
pub enum SubscriptionManagementAction {
    Subscribe,
    Unsubscribe,
}

impl SubscriptionManagementAction {
    pub fn as_str(&self) -> &'static str {
        match self {
            SubscriptionManagementAction::Subscribe => "SUBSCRIBE",
            SubscriptionManagementAction::Unsubscribe => "UNSUBSCRIBE",
        }
    }
    fn as_id(&self) -> u64 {
        match self {
            SubscriptionManagementAction::Subscribe => 1,
            SubscriptionManagementAction::Unsubscribe => 2,
        }
    }
    #[allow(dead_code)] // will be used once the Router is implemented
    pub fn from_id(id: u64) -> Option<Self> {
        match id {
            1 => Some(SubscriptionManagementAction::Subscribe),
            2 => Some(SubscriptionManagementAction::Unsubscribe),
            _ => None,
        }
    }
}


pub struct BinanceWebSocketActor {
    actor_id: String,
    command_receiver: mpsc::Receiver<WebSocketCommand>,
    to_orch: mpsc::Sender<WebSocketMessage>,
    #[allow(dead_code)] // will be used once the Router is implemented
    router_sender: mpsc::Sender<ExchangeMessage>,
    write: Option<SplitSink<WsStream, Message>>,
    read: Option<SplitStream<WsStream>>,
}

impl BinanceWebSocketActor {
    pub fn new(
        actor_id: String,
        command_receiver: mpsc::Receiver<WebSocketCommand>,
        to_orch: mpsc::Sender<WebSocketMessage>,
        router_sender: mpsc::Sender<ExchangeMessage>,
    ) -> Self {
        Self {
            actor_id,
            command_receiver,
            to_orch,
            router_sender,
            write: None,
            read: None,
        }
    }

    pub async fn run(mut self) {
        let span = tracing::info_span!(
            "BinanceWebSocketActor",
            actor_id = %self.actor_id,
        );

        async move {
            let mut heartbeat_interval =
                time::interval(Duration::from_secs(WEBSOCKET_HEARTBEAT_INTERVAL));
            

            if let Err(e) = self.connect_to_exchange().await {
                self.send_error_to_orch(e).await;
            }
            
            loop {
                tokio::select! {
                    _ = heartbeat_interval.tick() => {
                        self.send_heartbeat().await;
                    }
                    
                    Some(msg_result) = async {
                        if let Some(read) = self.read.as_mut() {
                            read.next().await
                        } else {
                            None
                        }
                    } => {
                        match msg_result {
                            Ok(msg) => info!("Binance message: {:?}", msg),
                            // Ok(_) => (),
                            Err(e) => error!("❌ Error reading from Binance WS: {:?}", e),
                        }
                    }

                    Some(cmd) = self.command_receiver.recv() => {
                        let should_continue = self.handle_command(cmd).await;
                        if !should_continue {
                            info!("Teardown requested, shutting down BinanceWebSocketActor");
                            self.teardown().await;
                            break;
                        }
                    }
                }
            }
        }
        .instrument(span)
        .await
    }

    async fn handle_command(&mut self, command: WebSocketCommand) -> bool {
        match command {
            WebSocketCommand::Subscribe(sub) => {
                if let Err(e) = self.subscribe(sub).await {
                    self.send_error_to_orch(e).await;
                }
                
                true
            }
            WebSocketCommand::Unsubscribe(sub) => {
                if let Err(e) = self.unsubscribe(sub).await {
                    self.send_error_to_orch(e).await;
                }
                true
            }
            WebSocketCommand::Resubscribe(sub) => {
                if let Err(e) = self.subscribe(sub).await {
                    self.send_error_to_orch(e).await;
                }
                true
            }
            WebSocketCommand::Teardown => {
                info!("Teardown received, shutting down connection...");
                false
            }
        }
    }

    async fn send_heartbeat(&self) -> bool {
        match self
            .to_orch
            .send(WebSocketMessage::Heartbeat {
                actor_id: self.actor_id.clone(),
            })
            .await
        {
            Ok(_) => {
                debug!("sent heartbeat"); // TODO convert to metric
                true
            }
            Err(e) => {
                error!("Failed to send heartbeat to orchestrator: {e}");
                false
            }
        }
    }

    async fn subscribe(
        &mut self,
        subscription: ExchangeSubscription,
    ) -> Result<(), WebSocketActorError> {
        info!("Subscribing to {}", subscription.exchange_stream_id);
        let streams = vec![subscription.exchange_stream_id];
        let message = build_binance_subscription_message(
            &streams,
            SubscriptionManagementAction::Subscribe
        );
        self.send_to_exchange(message).await?;
        Ok(())
    }

    async fn unsubscribe(
        &mut self,
        subscription: ExchangeSubscription,
    ) -> Result<(), WebSocketActorError> {
        info!("Unsubscribing to {}", subscription.exchange_stream_id);
        let streams = vec![subscription.exchange_stream_id];
        let message = build_binance_subscription_message(
            &streams,
            SubscriptionManagementAction::Unsubscribe
        );
        self.send_to_exchange(message).await?;
        Ok(())
    }

    async fn send_to_exchange(
        &mut self,
        message: String
    ) -> Result<(), WebSocketActorError> {
        if let Some(write) = &mut self.write {
            match write.send(Message::Text(message.clone().into())).await {
                Ok(_) => {
                    info!("Successfully sent message to exchange: {:?}", message);
                    Ok(())
                }
                Err(e) => {
                    error!("Failed to send message to exchange: {e}");
                    Err(WebSocketActorError::Send(e.to_string()))
                }
            }
        } else {
            error!("Write stream is not initialised");
            Err(WebSocketActorError::WriteNotInitialised)
        }
    }

    async fn send_error_to_orch(&self, error: WebSocketActorError) {
        if let Err(send_err) = self.to_orch.send(WebSocketMessage::Error {
            actor_id: self.actor_id.clone(),
            error,
        }).await {
            error!("Failed to send error to orchestrator: {send_err}");
        }
    }
    
    async fn teardown(&mut self) {
        info!("🧹 BinanceWebSocketActor {}: initiating teardown...", self.actor_id);

        if let Some(write) = &mut self.write {
            match timeout(Duration::from_secs(3), write.send(Message::Close(None))).await {
                Ok(Ok(())) => info!("✅ Sent close frame"),
                Ok(Err(e)) => warn!("⚠️ Failed to send close frame: {:?}", e),
                Err(_) => warn!("⚠️ Timeout sending close frame"),
            }
        } else {
            warn!("⚠️ Write stream not initialized, skipping close frame");
        }

        self.write.take();
        self.read.take();
        if let Err(e) = self.to_orch.send(WebSocketMessage::Shutdown {
            actor_id: self.actor_id.clone()
        }).await {
            error!("❌ Failed to notify orchestrator of shutdown: {e}");
        } else {
            info!("✅ Shutdown message sent to orchestrator");
        }
    }

    async fn connect_to_exchange(&mut self) -> Result<(), WebSocketActorError> {
        info!("Connecting to Binance Spot WS: {}", BINANCE_SPOT_WS_URL);
        match connect_async(BINANCE_SPOT_WS_URL).await {
            Ok((stream, _)) => {
                let(write, read) = stream.split();
                self.write = Some(write);
                self.read = Some(read);
                Ok(())
            }
            Err(e) => {
                error!("Failed to connect to Binance Spot WS: {}", e);
                Err(WebSocketActorError::Connection(e.to_string()))
            }
        }
    }
}


fn build_binance_subscription_message(streams: &[String], action: SubscriptionManagementAction) -> String {
    let payload = serde_json::json!({
        "method": action.as_str(),
        "params": streams,
        "id": action.as_id(),
    });
    payload.to_string()
}