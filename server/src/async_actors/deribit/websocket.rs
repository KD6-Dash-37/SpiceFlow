use crate::async_actors::messages::{ExchangeMessage, WebSocketCommand, WebSocketMessage};
use crate::async_actors::subscription::ExchangeSubscription;
// Type alias for WebSocket stream
type WsStream = WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;

use futures_util::stream::{SplitSink, SplitStream, StreamExt};
use futures_util::SinkExt;
use once_cell::sync::Lazy;
use serde::Serialize;
use tokio::sync::mpsc;
use tokio::time::{self, sleep, timeout, Duration};
use tokio_tungstenite::{
    connect_async,
    tungstenite::{protocol::Message, Error as TungsteniteError},
    MaybeTlsStream, WebSocketStream,
};
use tracing::{debug, error, warn, info, Instrument};

const WEBSOCKET_HEARTBEAT_INTERVAL: u64 = 5;
const WEBSOCKET_STAY_ALIVE_INTERVAL: u64 = 5;
const DEFAULT_JSONRPC: &str = "2.0";
const SEND_CLOSE_FRAME_TIMEOUT: u64 = 3;
const ORCH_RETRY_ATTEMPTS: u8 = 3;
const DERIBIT_WS_BASE_URL: &str = "wss://www.deribit.com/ws/api/v2";

#[derive(Debug)]
pub enum SubscriptionManagementAction {
    Subscribe,
    Unsubscribe,
    UnsubscribeAll,
    Test,
}

impl SubscriptionManagementAction {
    pub fn as_str(&self) -> &'static str {
        match self {
            SubscriptionManagementAction::Subscribe => "public/subscribe",
            SubscriptionManagementAction::Unsubscribe => "public/unsubscribe",
            SubscriptionManagementAction::UnsubscribeAll => "public/unsubscribe_all",
            SubscriptionManagementAction::Test => "public/test",
        }
    }
    fn as_id(&self) -> u64 {
        match self {
            SubscriptionManagementAction::Subscribe => 1,
            SubscriptionManagementAction::Unsubscribe => 2,
            SubscriptionManagementAction::UnsubscribeAll => 21,
            SubscriptionManagementAction::Test => 9999,
        }
    }
    pub fn from_id(id: u64) -> Option<Self> {
        match id {
            1 => Some(SubscriptionManagementAction::Subscribe),
            2 => Some(SubscriptionManagementAction::Unsubscribe),
            21 => Some(SubscriptionManagementAction::UnsubscribeAll),
            9999 => Some(SubscriptionManagementAction::Test),
            _ => None,
        }
    }
}

#[derive(Serialize)]
struct SubscriptionRequest<'a> {
    jsonrpc: &'a str,
    id: Option<u64>,
    method: &'a str,
    params: SubscriptionRequestParams<'a>,
}

#[derive(Serialize)]
struct SubscriptionRequestParams<'a> {
    channels: &'a [String],
}

static STAY_ALIVE_MESSAGE: Lazy<Message> = Lazy::new(|| {
    let json = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 9999,
        "method": "public/test",
        "params": {}
    });
    Message::Text(json.to_string().into())
});

static UNSUBSCRIBE_ALL_MESSAGE: Lazy<Message> = Lazy::new(|| {
    let json = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 21,
        "method": "public/unsubscribe_all",
        "params": {}
    });
    Message::Text(json.to_string().into())
});

#[derive(Debug)]
pub enum WebSocketActorError {
    SendError(String),
    WriteNotInitialised,
    Timeout,
    ReceiveError(String),
    ConnectionError(String),
}

pub struct DeribitWebSocketActor {
    command_receiver: mpsc::Receiver<WebSocketCommand>,
    to_orch: mpsc::Sender<WebSocketMessage>,
    router_sender: mpsc::Sender<ExchangeMessage>,
    actor_id: String,
    exchange_write: Option<SplitSink<WsStream, Message>>,
    exchange_read: Option<SplitStream<WsStream>>,
}

impl DeribitWebSocketActor {
    pub fn new(
        command_receiver: mpsc::Receiver<WebSocketCommand>,
        to_orch: mpsc::Sender<WebSocketMessage>,
        router_sender: mpsc::Sender<ExchangeMessage>,
        actor_id: String,
    ) -> Self {
        Self {
            command_receiver,
            to_orch,
            router_sender,
            actor_id,
            exchange_write: None,
            exchange_read: None,
        }
    }

    pub async fn run(mut self) {
        let span = tracing::info_span!(
            "DeribitWebSocketActor",
            actor_id = %self.actor_id
        );

        async move {
            let mut heartbeat_interval =
                time::interval(Duration::from_secs(WEBSOCKET_HEARTBEAT_INTERVAL));
            let mut stay_alive_interval =
                time::interval(Duration::from_secs(WEBSOCKET_STAY_ALIVE_INTERVAL));
            let _ = self.create_ws_stream().await;

            loop {
                tokio::select! {

                    Some(exchange_message_result) = async {
                        if let Some(exchange_read) = self.exchange_read.as_mut() {
                            exchange_read.next().await
                        } else {
                            None
                        }
                    } => {
                        match exchange_message_result {
                            Ok(message) => {
                                if let Err(_) = self.router_sender.send(ExchangeMessage::new(self.actor_id.clone(), message)).await {
                                    warn!("Failed to forward message to router");
                                }
                            }
                            Err(e) => {
                                error!("Error receiving exchange message: {:?}", e);
                            }
                        }
                    }

                    Some(cmd) = self.command_receiver.recv() => {
                        let should_continue = self.handle_command(cmd).await;
                        if !should_continue {
                            break;
                        }
                    }

                    _ = heartbeat_interval.tick() => {
                        if !self.send_heartbeat().await {
                            break;
                        }
                    }

                    _ = stay_alive_interval.tick() => {
                        match self.stay_alive().await {
                            Ok(()) => {
                                debug!("Stay-Alive successful"); // TODO demote to debug, later metric
                            },
                            Err(err) => {
                                warn!("Stay-Alive failed: {:?}", err);
                                let _ = self.send_message_to_orch(WebSocketMessage::Disconnected { actor_id: self.actor_id.clone() }).await;
                                break;
                            }
                        }
                    }
                }
            }
        }
        .instrument(span) // Attach the span to the entire async execution
        .await
    }

    async fn send_heartbeat(&mut self) -> bool {
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

    async fn create_ws_stream(&mut self) -> Result<(), WebSocketActorError> {
        debug!("Attempting to create WebSocket stream to {DERIBIT_WS_BASE_URL}");
        match connect_with_retry(DERIBIT_WS_BASE_URL, None, 10).await {
            Ok(ws_stream) => {
                debug!("WS connection established to {DERIBIT_WS_BASE_URL}");
                let (write_half, read_half) = ws_stream.split();
                self.exchange_write = Some(write_half);
                self.exchange_read = Some(read_half);
                Ok(())
            }
            Err(err) => {
                error!("Failed to create WebSocket stream: {err}");
                Err(WebSocketActorError::ConnectionError(err.to_string()))
            }
        }
    }

    async fn send_message_to_orch(&self, mut message: WebSocketMessage) {
        for _ in 0..ORCH_RETRY_ATTEMPTS {
            match self.to_orch.try_send(message) {
                Ok(_) => {
                    debug!("✅ Successfully sent message to orchestrator");
                    return;
                }
                Err(tokio::sync::mpsc::error::TrySendError::Full(m)) => {
                    warn!("Orchestrator channel is full, retrying...");
                    message = m;
                    sleep(Duration::from_millis(100)).await;
                }
                Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                    error!("❌ Orchestrator channel is closed, unable to send message");
                    return;
                }
            }
        }
        error!(
            "❌ Failed to send message to orchestrator after {ORCH_RETRY_ATTEMPTS} retries, dropping message",
        );
    }

    async fn handle_command(&mut self, command: WebSocketCommand) -> bool {
        match command {
            WebSocketCommand::Subscribe(subscription) => {
                self.subscribe(subscription).await;
                true
            }
            WebSocketCommand::Unsubscribe(subscription) => {
                self.unsubscribe(subscription).await;
                true
            }
            WebSocketCommand::Resubscribe(subscription) => {
                self.unsubscribe(subscription.clone()).await;
                self.subscribe(subscription).await;
                true
            }
            WebSocketCommand::Teardown => {
                self.tear_down().await;
                false
            }
        }
    }

    async fn subscribe(&mut self, subscription: ExchangeSubscription) {
        match subscription {
            ExchangeSubscription::Deribit(sub) => {
                info!("Subscribing to {}", sub.stream_id);
                let channels = [sub.exchange_stream_id.clone()];
                if let Some(message) = prepare_subscription_management_message(
                    SubscriptionManagementAction::Subscribe,
                    &channels,
                ) {
                    let _ = self.send_message_to_exchange(message).await;
                } else {
                    error!("Failed to prepare subscription message");
                }
            }
        }
    }

    async fn unsubscribe(&mut self, subscription: ExchangeSubscription) {
        match subscription {
            ExchangeSubscription::Deribit(sub) => {
                debug!("Unsubscribing from {}", sub.stream_id);
                let channels = vec![sub.exchange_stream_id.clone()];

                if let Some(message) = prepare_subscription_management_message(
                    SubscriptionManagementAction::Unsubscribe,
                    &channels,
                ) {
                    let _ = self.send_message_to_exchange(message).await;
                } else {
                    error!("Failed to preperate unsubscribe message");
                }
            }
        }
    }

    async fn send_message_to_exchange(
        &mut self,
        message: String,
    ) -> Result<(), WebSocketActorError> {
        if let Some(write) = &mut self.exchange_write {
            match write.send(Message::Text(message.clone().into())).await {
                Ok(_) => {
                    debug!("Successfully sent message to exchange: {:?}", message);
                    Ok(())
                }
                Err(e) => {
                    error!("Failed to send message to exchange: {e}");
                    Err(WebSocketActorError::SendError(e.to_string()))
                }
            }
        } else {
            error!("Write stream is not initialised");
            Err(WebSocketActorError::WriteNotInitialised)
        }
    }

    async fn stay_alive(&mut self) -> Result<(), WebSocketActorError> {
        debug!("Sending Stay-Alive message...");
        if let Some(write) = &mut self.exchange_write {
            match write.send(STAY_ALIVE_MESSAGE.clone()).await {
                Ok(_) => {
                    debug!("Stay-Alive message sent to exchange");
                    Ok(())
                }
                Err(e) => {
                    error!(actor_id = %self.actor_id,"Failed to send Stay-Alive message: {e}");
                    Err(WebSocketActorError::SendError(e.to_string()))
                }
            }
        } else {
            warn!("Cannot send Stay-Alive message, write stream not initialised");
            Err(WebSocketActorError::WriteNotInitialised)
        }
    }

    async fn unsubscribe_all(&mut self) -> Result<(), WebSocketActorError> {
        debug!("Sending unsubscribe_all for public streams");
        if let Some(write) = &mut self.exchange_write {
            write
                .send(UNSUBSCRIBE_ALL_MESSAGE.clone())
                .await
                .map_err(|e| WebSocketActorError::SendError(e.to_string()))
        } else {
            Err(WebSocketActorError::WriteNotInitialised)
        }
    }

    async fn send_close_frame(&mut self) -> Result<(), WebSocketActorError> {
        debug!("Sending close frame");

        if let Some(write) = &mut self.exchange_write {
            match timeout(
                Duration::from_secs(SEND_CLOSE_FRAME_TIMEOUT),
                write.send(Message::Close(None)),
            )
            .await
            {
                Ok(Err(err)) => {
                    warn!("Failed to send WebSocket close frame: {:?}", err);
                    Err(WebSocketActorError::SendError(err.to_string()))
                }
                Err(_) => {
                    warn!("Timeout while waiting to send WebSocket close frame");
                    Err(WebSocketActorError::Timeout)
                }
                Ok(Ok(())) => {
                    debug!("Successfully sent WebSocket close frame");
                    Ok(())
                }
            }
        } else {
            warn!("Write stream was not initialised, skipping close frame");
            Err(WebSocketActorError::WriteNotInitialised)
        }
    }

    fn drop_exchange_streams(&mut self) {
        debug!("Dropping exchange streams");
        if self.exchange_write.take().is_none() {
            warn!("Exchange_write stream was already None");
        } else {
            debug!("Successfully dropped exchange_write");
        }

        if self.exchange_read.take().is_none() {
            warn!("Exchange_read stream was already None");
        } else {
            debug!("Successfully dropped exchange_read");
        }
    }

    async fn tear_down(&mut self) {
        debug!("{}: Initiating teardown sequence", self.actor_id);

        // Attempt to unsubscribe to all subscriptions, regardless if you there are any active ones
        if let Err(err) = self.unsubscribe_all().await {
            error!("Failed to send unsubscribe_all message: {:?}", err);
        } else {
            debug!("Successfully sent unsubscribe_all message");
        }

        let _ = self.send_close_frame().await;

        // Drop exchange streams
        self.drop_exchange_streams();

        self.send_message_to_orch(WebSocketMessage::Shutdown {
            actor_id: self.actor_id.clone(),
        })
        .await;

        debug!("Teardown complete");
    }
}

fn prepare_subscription_management_message(
    action: SubscriptionManagementAction,
    channels: &[String],
) -> Option<String> {
    let message = SubscriptionRequest {
        jsonrpc: DEFAULT_JSONRPC,
        id: Some(action.as_id()),
        method: &action.as_str(),
        params: SubscriptionRequestParams { channels },
    };
    serde_json::to_string(&message).ok()
}

async fn connect_with_retry(
    url: &str,
    max_retries: Option<u32>,
    max_backoff_sec: u64,
) -> Result<WsStream, TungsteniteError> {
    let mut attempt = 0;
    let mut backoff_sec = 1;

    loop {
        match connect_async(url).await {
            Ok((ws_stream, _response)) => {
                // Successfully connected
                return Ok(ws_stream);
            }
            Err(e) => {
                attempt += 1;
                // If we have a max retry limit and we've hit it, stop and return the error
                if let Some(max) = max_retries {
                    if attempt >= max {
                        error!("Reached maximum number of retries ({max}).");
                        return Err(e);
                    }
                }

                warn!(
                    "Failed to connect on attempt {attempt}: {e}. \
                     Retrying in {backoff_sec}s..."
                );
                // Wait for the backoff duration
                sleep(Duration::from_secs(backoff_sec)).await;
                // Increase the backoff time for the next attempt
                backoff_sec = (backoff_sec * 2).min(max_backoff_sec);
            }
        }
    }
}
