use std::collections::HashMap;
use tokio::sync::mpsc;
use super::ws::{WebSocketActor, WebSocketCommand};

#[derive(Debug)]
pub struct Orchestrator {
    actor_registry: HashMap<String, ActorMetaData>,
    message_receiver: mpsc::Receiver<OrchestratorMessage>,
}

#[derive(Debug, Clone)]
struct ActorMetaData {
    #[warn(dead_code)]
    actor_type: String,
    #[warn(dead_code)]
    config: ActorConfig,
}

#[derive(Debug, Clone)]
pub enum RequestedFeed {
    OrderBook,
}

#[derive(Debug, Clone)]
pub struct ActorConfig {
    pub actor_id: String,
    pub internal_symbol: String,
    pub exchange_symbol: String,
    pub channels: Vec<String>,
    pub requested_feed: RequestedFeed,
}

pub enum OrchestratorMessage {
    CreateWebSocketActor {instrument: String},
    StopActor {actor_id: String},
    GetActorState {actor_id: String}
}

impl Orchestrator {

    pub fn new(message_receiver: mpsc::Receiver<OrchestratorMessage>) -> Self {
        Self {
            actor_registry: HashMap::new(),
            message_receiver,
        }
    }

    pub async fn run(mut self) {
        while let Some(message) = self.message_receiver.recv().await {
            match message {
                OrchestratorMessage::CreateWebSocketActor { instrument } => {
                    println!("Creating WebSocket actor for {}", instrument);
                    self.create_websocket_actor(instrument).await;
                }
                OrchestratorMessage::StopActor { actor_id } => {
                    println!("Stopping actor with ID: {}", actor_id);
                    self.stop_actor(actor_id).await;
                }
                OrchestratorMessage::GetActorState { actor_id } => {
                    println!("Fetching state for actor ID: {}", actor_id);
                    self.get_actor_state(actor_id).await;
                }
            }
        }
    }

    // Stub for creating a WebSocket actor
    async fn create_websocket_actor(&mut self, instrument: String) {
        let actor_id = format!("WebSocketActor-{}", instrument);
        
        let actor_metadata = ActorMetaData {
            actor_type: "WebSocketActor".to_string(),
            config: ActorConfig { 
                actor_id: actor_id.clone(),
                exchange_symbol: "BTC-PERPETUAL".to_string(),
                internal_symbol: "Deribit.InvFut.BTC.USD".to_string(),
                requested_feed: RequestedFeed::OrderBook,
                channels: vec!["book.BTC-PERPETUAL.100ms".to_string()],
             }
        };
        self.actor_registry.insert(actor_id.clone(), actor_metadata);
        println!("Actor created: {}", actor_id);
    }

    pub fn spawn_websocket_actor(
        &mut self,
        router_write: mpsc::Sender<String>,
        ws_command_read: mpsc::Receiver<WebSocketCommand>
    ) {
        
        let actor_id = "WebSocketActor".to_string();
        let actor_config = ActorConfig { 
            actor_id: actor_id.clone(),
            exchange_symbol: "BTC-PERPETUAL".to_string(),
            internal_symbol: "Deribit.InvFut.BTC.USD".to_string(),
            requested_feed: RequestedFeed::OrderBook,
            channels: vec!["book.BTC-PERPETUAL.100ms".to_string()],
         };

        // Check if actor already exists in the registry
        if self.actor_registry.contains_key(&actor_id) {
            println!("Actor: {} already exists, skipping creation.", actor_id);
            return;
        }

        // Add to the actor registry
        let metadata = ActorMetaData {
            actor_type: "WebSocketActor".to_string(),
            config: actor_config.clone(),
        };
        self.actor_registry.insert(actor_id.clone(), metadata);

        // Spawn the WebSocketActor as a tokio task
        tokio::spawn(async move {
            let mut websocket_actor = WebSocketActor::new(
                actor_config,
                "WebSocketActor",
                router_write,
                ws_command_read
            );
            websocket_actor.run().await;
        });
        print!("WebSocketActor spawned by Orchestrator")
    }

    // Stub for stopping an actor
    async fn stop_actor(&mut self, actor_id: String) {
        if self.actor_registry.remove(&actor_id).is_some() {
            println!("Actor {} stopped and removed from registry", actor_id);
        } else {
            println!("Actor {} not found in registry", actor_id);
        }
    }

    // Stub for fetching actor state
    async fn get_actor_state(&self, actor_id: String) {
        if let Some(metadata) = self.actor_registry.get(&actor_id) {
            println!("Actor state: {:?}", metadata);
        } else {
            println!("Actor {} not found in registry", actor_id);
        }
    }
}
