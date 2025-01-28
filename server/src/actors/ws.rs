use super::orch::ActorConfig;
use futures_util::stream::SplitSink;
use futures_util::{SinkExt, StreamExt};
use log;
use once_cell::sync::Lazy;
use serde;
use serde::Serialize;
use tokio::sync::mpsc;
use tokio::time::{sleep, Duration};
use tokio_tungstenite::{
    connect_async, tungstenite::protocol::Message, tungstenite::Error as TungsteniteError,
    MaybeTlsStream, WebSocketStream,
};

type WsStream = WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;

#[derive(Debug)]
pub enum WebSocketCommand {
    Resubscribe,
    // Stop,
}

#[derive(Debug)]
pub enum SubscriptionManagementAction {
    Subscribe,
    Unsubscribe,
    Test,
}

impl SubscriptionManagementAction {
    fn as_str(&self) -> &'static str {
        match self {
            SubscriptionManagementAction::Subscribe => "public/subscribe",
            SubscriptionManagementAction::Unsubscribe => "public/unsubscribe",
            SubscriptionManagementAction::Test => "public/test",
        }
    }
    fn as_id(&self) -> u64 {
        match self {
            SubscriptionManagementAction::Subscribe => 1,
            SubscriptionManagementAction::Unsubscribe => 2,
            SubscriptionManagementAction::Test => 9999,
        }
    }
    pub fn from_id(id: u64) -> Option<Self> {
        match id {
            1 => Some(SubscriptionManagementAction::Subscribe),
            2 => Some(SubscriptionManagementAction::Unsubscribe),
            9999 => Some(SubscriptionManagementAction::Test),
            _ => None,
        }
    }
}

const DEFAULT_JSONRPC: &str = "2.0";

static STAY_ALIVE_MESSAGE: Lazy<Message> = Lazy::new(|| {
    let json = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 9999,
        "method": "public/test",
        "params": {}
    });
    Message::Text(json.to_string().into())
});

#[derive(Serialize)]
struct SubscriptionRequest<'a> {
    jsonrpc: &'a str,
    id: Option<u64>,
    method: &'a str,
    params: SubscriptionRequestParams<'a>,
}

#[derive(Serialize)]
struct SubscriptionRequestParams<'a> {
    channels: &'a Vec<String>,
}

pub struct WebSocketActor {
    actor_config: ActorConfig,
    name: String,
    exchange_write: Option<SplitSink<WsStream, Message>>,
    exchange_read: Option<futures_util::stream::SplitStream<WsStream>>,
    router_write: mpsc::Sender<String>,
    ws_command_read: mpsc::Receiver<WebSocketCommand>,
}

impl WebSocketActor {
    // Creates a new WebSocketActor with a name and a list of channels to subscribe to.
    pub fn new(
        actor_config: ActorConfig,
        name: &str,
        router_write: mpsc::Sender<String>,
        ws_command_read: mpsc::Receiver<WebSocketCommand>,
    ) -> Self {
        Self {
            actor_config,
            name: name.to_string(),
            exchange_write: None,
            exchange_read: None,
            router_write,
            ws_command_read,
        }
    }

    pub async fn run(&mut self) {
        self.create_ws_stream().await;
        self.subscribe().await;

        // Create an interval for the stay-alive mechanism
        let mut stay_alive_interval = tokio::time::interval(Duration::from_secs(30));

        loop {
            tokio::select! {
                // Handle incoming commands from the WebSocket command receiver
                Some(command) = self.ws_command_read.recv() => {
                    self.handle_command(command).await;
                }

                // Continuously read and forward WebSocket messages
                result = self.exchange_read.as_mut().unwrap().next() => {
                    match result {
                        // Handle the case where the stream returns `Some(Result::Ok)`
                        Some(Ok(Message::Text(text))) => {
                            if let Err(err) = self.router_write.send(text.to_string()).await {
                                log::warn!("{}: Failed to forward message to OrderBookActor: {}", self.name, err);
                            }
                        }
                        // Handle the case where the stream returns `Some(Result::Ok)` for other message types
                        Some(Ok(other_message)) => {
                            log::warn!("{}: Unhandled WebSocket message type: {:?}", self.name, other_message);
                        }
                        // Handle the case where the stream returns `Some(Result::Err)`
                        Some(Err(err)) => {
                            log::error!("{}: Error reading WS message: {}", self.name, err);
                            break; // Exit the loop on WebSocket error
                        }
                        // Handle the case where the stream ends (`None`)
                        None => {
                            log::warn!("{}: WebSocket stream ended unexpectedly", self.name);
                            break; // Exit the loop and reconnect
                        }
                    }
                }

                // Send periodic stay-alive message
                _ = stay_alive_interval.tick() => {
                    self.stay_alive().await;
                }
            }
        }
    }

    async fn create_ws_stream(&mut self) {
        let url = "wss://www.deribit.com/ws/api/v2"; // TODO extract the hardcoded URL, config perhaps?
        log::info!(
            "{}: Attempting to create WebSocket stream to {}",
            self.name,
            url
        );
        match connect_with_retry(url, None, 10).await {
            Ok(ws_stream) => {
                log::info!("{}: WS connection established to {}", self.name, url);
                let (_write, _read) = ws_stream.split();
                self.exchange_write = Some(_write);
                self.exchange_read = Some(_read)
            }
            Err(err) => {
                log::error!("{}: Failed to create WebSocket stream: {}", self.name, err);
                panic!(
                    "{}: Critical failure - unable to establish WebSocket connection",
                    self.name
                );
            }
        }
    }

    async fn handle_command(&mut self, command: WebSocketCommand) {
        match command {
            WebSocketCommand::Resubscribe => {
                log::info!("{}: Resubscribing to order book", self.name);
                self.unsubscribe().await;
                self.subscribe().await;
            }
        }
    }

    async fn subscribe(&mut self) {
        // let config = StreamConfig::current();
        log::info!("{}: Subscribing to {}", self.name, self.actor_config.internal_symbol);

        // Check if write is initialised
        if self.exchange_write.is_none() {
            log::error!("{}: Write stream is not initialised", self.name);
            return;
        }

        if let Some(write) = &mut self.exchange_write {
            if let Some(message) = WebSocketActor::prepare_subscription_management_message(
                SubscriptionManagementAction::Subscribe,
                &self.actor_config.channels,
            ) {
                if let Err(err) = write.send(Message::Text(message.into())).await {
                    log::error!(
                        "{}: Failed to send subscription message: {}",
                        self.name,
                        err
                    );
                    return;
                }

                log::info!(
                    "{}: Subscription message sent for channels: {:?}",
                    self.name,
                    self.actor_config.channels
                );
            } else {
                log::error!("{}: Failed to prepare subscription message", self.name);
            }
        }
    }

    async fn unsubscribe(&mut self) {
        log::info!("{}: Unsubscribing to {}", self.name, self.actor_config.internal_symbol);

        // Check if write is initialised
        if self.exchange_write.is_none() {
            log::error!("{}: Write stream is not initialised", self.name);
            return;
        }

        if let Some(write) = &mut self.exchange_write {
            if let Some(message) = WebSocketActor::prepare_subscription_management_message(
                SubscriptionManagementAction::Unsubscribe,
                &self.actor_config.channels,
            ) {
                if let Err(err) = write.send(Message::Text(message.into())).await {
                    log::error!("{}: Failed to send unsubscripe message: {}", self.name, err);
                    return;
                }

                log::info!(
                    "{}: Unsubscribe message sent for channels: {:?}",
                    self.name,
                    self.actor_config.channels
                );
            } else {
                log::error!("{}: Failed to prepare unsubscribe message", self.name);
            }
        }
    }

    fn prepare_subscription_management_message(
        action: SubscriptionManagementAction,
        channels: &Vec<String>,
    ) -> Option<String> {
        let message = SubscriptionRequest {
            jsonrpc: DEFAULT_JSONRPC,
            id: Some(action.as_id()),
            method: action.as_str(),
            params: SubscriptionRequestParams { channels },
        };
        serde_json::to_string(&message).ok()
    }

    async fn stay_alive(&mut self) {
        if let Some(write) = &mut self.exchange_write {
            // Send pre-defined stay-alive message
            if let Err(err) = write.send(STAY_ALIVE_MESSAGE.clone()).await {
                log::warn!(
                    "{}: Failed to send stay-alive test message: {}",
                    self.name,
                    err
                );
            } else {
                log::debug!("{}: Stay-alive message sent", self.name);
            }
        } else {
            log::error!(
                "{}: Cannot send stay-alive test mesage, write stream is not initialised",
                self.name
            )
        }
    }
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
                        eprintln!("Reached maximum number of retries ({max}).");
                        return Err(e);
                    }
                }

                eprintln!(
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
