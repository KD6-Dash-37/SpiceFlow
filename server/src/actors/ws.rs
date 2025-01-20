use super::stream_config::StreamConfig;
use futures_util::stream::SplitSink;
use futures_util::{SinkExt, StreamExt};
use log::{error, info, warn};
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
    // Stop, For potential future use, e.g. gracefully stopping the WebSocketActor
}

const DEFAULT_JSONRPC: &str = "2.0";

#[derive(Serialize)]
struct SubscriptionMessage<'a> {
    jsonrpc: &'a str,
    method: &'a str,
    params: SubscriptionParams<'a>,
}

#[derive(Serialize)]
struct SubscriptionParams<'a> {
    channels: &'a Vec<String>,
}

#[derive(Serialize)]
struct UnsubscriptionMessage<'a> {
    jsonrpc: &'a str,
    method: &'a str,
    params: UnsubscriptionParams<'a>,
}

#[derive(Serialize)]
struct UnsubscriptionParams<'a> {
    channels: &'a Vec<String>,
}

pub struct WebSocketActor {
    name: String,
    write: Option<SplitSink<WsStream, Message>>,
    read: Option<futures_util::stream::SplitStream<WsStream>>,
    router: mpsc::Sender<String>,
    ws_command_receiver: mpsc::Receiver<WebSocketCommand>,
}

impl WebSocketActor {
    // Creates a new WebSocketActor with a name and a list of channels to subscribe to.
    pub fn new(
        name: &str,
        router: mpsc::Sender<String>,
        ws_command_receiver: mpsc::Receiver<WebSocketCommand>,
    ) -> Self {
        Self {
            name: name.to_string(),
            write: None,
            read: None,
            router,
            ws_command_receiver,
        }
    }

    pub async fn run(&mut self) {
        self.create_ws_stream().await;
        self.subscribe().await;

        loop {
            tokio::select! {
                // Handle incoming commands from the WebSocket command receiver
                Some(command) = self.ws_command_receiver.recv() => {
                    self.handle_command(command).await;
                }

                // Continuously read and forward WebSocket messages
                result = self.read.as_mut().unwrap().next() => {
                    match result {
                        // Handle the case where the stream returns `Some(Result::Ok)`
                        Some(Ok(Message::Text(text))) => {
                            if let Err(err) = self.router.send(text.to_string()).await {
                                warn!("{}: Failed to forward message to OrderBookActor: {}", self.name, err);
                            }
                        }
                        // Handle the case where the stream returns `Some(Result::Ok)` for other message types
                        Some(Ok(other_message)) => {
                            warn!("{}: Unhandled WebSocket message type: {:?}", self.name, other_message);
                        }
                        // Handle the case where the stream returns `Some(Result::Err)`
                        Some(Err(err)) => {
                            error!("{}: Error reading WS message: {}", self.name, err);
                            break; // Exit the loop on WebSocket error
                        }
                        // Handle the case where the stream ends (`None`)
                        None => {
                            warn!("{}: WebSocket stream ended unexpectedly", self.name);
                            break; // Exit the loop and reconnect
                        }
                    }
                }
            }
        }
    }

    async fn create_ws_stream(&mut self) {
        let url = "wss://www.deribit.com/ws/api/v2"; // TODO extract the hardcoded URL, config perhaps?
        info!(
            "{}: Attempting to create WebSocket stream to {}",
            self.name, url
        );
        match connect_with_retry(url, None, 10).await {
            Ok(ws_stream) => {
                info!("{}: WS connection established to {}", self.name, url);
                let (_write, _read) = ws_stream.split();
                self.write = Some(_write);
                self.read = Some(_read)
            }
            Err(err) => {
                error!("{}: Failed to create WebSocket stream: {}", self.name, err);
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
                info!("{}: Resubscribing to order book", self.name);
                self.unsubscribe().await;
                self.subscribe().await;
            }
        }
    }

    async fn subscribe(&mut self) {
        let config = StreamConfig::current();
        info!("{}: Subscribing to {}", self.name, config.internal_symbol);
        if let Some(write) = &mut self.write {
            // Prepare subscription message
            let subscription_message = SubscriptionMessage {
                jsonrpc: DEFAULT_JSONRPC,
                method: "public/subscribe",
                params: SubscriptionParams {
                    channels: &config.channels,
                },
            };

            // Serialise subscription message
            let message_text = match serde_json::to_string(&subscription_message) {
                Ok(json) => json,
                Err(err) => {
                    error!(
                        "{}: Failed to serialise subscription message: {}",
                        self.name, err
                    );
                    return;
                }
            };

            // Send subscription Message
            if let Err(err) = write
                .send(Message::Text(message_text.to_string().into()))
                .await
            {
                error!(
                    "{}: Failed to send subscription message: {}",
                    self.name, err
                );
                return;
            }

            info!(
                "{}: Subscription message sent, await response...",
                self.name
            );

            // Await a response from the WS server
            if let Some(read) = &mut self.read {
                match read.next().await {
                    Some(Ok(Message::Text(response))) => {
                        info!(
                            "{}: Received subscription response: {}",
                            self.name, response
                        );
                    }
                    Some(Ok(other_message)) => {
                        warn!(
                            "{} Unexpected WS message during subscription: {:?}",
                            self.name, other_message
                        );
                    }
                    Some(Err(err)) => {
                        error!(
                            "{}: Error reading WS response during subscription: {}",
                            self.name, err
                        );
                    }
                    None => {
                        warn!(
                            "{}: WS stream ended unexpectedly during subscription",
                            self.name
                        );
                    }
                }
            } else {
                error!("{}: Read stream is not initialised!", self.name)
            }
        } else {
            error!("{}: Write stream is not initialised!", self.name)
        }
    }

    async fn unsubscribe(&mut self) {
        info!("{}: Unsubscribing", self.name);
        let config = StreamConfig::current();
        if let Some(write) = &mut self.write {
            // Prepare unsubscribe message
            let unsubscribe_message = UnsubscriptionMessage {
                jsonrpc: DEFAULT_JSONRPC,
                method: "public/unsubscribe",
                params: UnsubscriptionParams {
                    channels: &config.channels,
                },
            };

            // Serialise unsubscribe message
            let message_text = match serde_json::to_string(&unsubscribe_message) {
                Ok(json) => json,
                Err(err) => {
                    error!(
                        "{}: Failed to serialise unsubscribe message: {}",
                        self.name, err
                    );
                    return;
                }
            };

            if let Err(err) = write
                .send(Message::Text(message_text.to_string().into()))
                .await
            {
                error!("{}: Failed to send unsubscribe message: {}", self.name, err);
                return;
            }

            info!("{}: Unsubscribe message sent, await response...", self.name);

            if let Some(read) = &mut self.read {
                match read.next().await {
                    Some(Ok(Message::Text(response))) => {
                        info!("{}: Received unsubscribe response: {}", self.name, response);
                    }
                    Some(Ok(other_message)) => {
                        warn!(
                            "{} Unexpected WS message during unsubscribe: {:?}",
                            self.name, other_message
                        );
                    }
                    Some(Err(err)) => {
                        error!(
                            "{}: Error reading WS response during unsubscribe: {}",
                            self.name, err
                        );
                    }
                    None => {
                        warn!(
                            "{}: WS stream ended unexpectedly during unsubscribe",
                            self.name
                        );
                    }
                }
            } else {
                error!("{}: Read stream is not initialised!", self.name)
            }
        } else {
            error!("{}: Write stream is not initialised", self.name);
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
