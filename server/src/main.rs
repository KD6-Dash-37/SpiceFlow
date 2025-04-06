// 🌍 Standard library
use std::sync::Arc;

// 📦 External crates
use dotenv::dotenv;
use tokio::{signal, sync::mpsc};
use tracing::info;

// 🧠 Internal modules
use server::async_actors::orchestrator::Orchestrator;
use server::domain::RefDataService;
use server::http_api::{handle::SubscriptionAction, start_http_server, OrchestratorHandle};

#[tokio::main]
async fn main() {
    dotenv().ok();

    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .expect("Failed to install default TLS provider");

    // Logging configuration
    tracing_subscriber::fmt::init();

    let (request_sender, request_receiver) = mpsc::channel::<SubscriptionAction>(32);
    // Initialise the orchestrator with the dummy gRPC receiver
    let orchestrator = Orchestrator::new(request_receiver);

    // Spawn the orchestrator's main loop
    tokio::spawn(async move {
        orchestrator.run().await;
    });

    let orchestrator_handle = OrchestratorHandle::new(request_sender);
    let ref_data = Arc::new(RefDataService::with_all_providers());

    tokio::spawn(async move {
        start_http_server(orchestrator_handle, ref_data).await;
    });

    info!("🌱 System started. Press Ctrl+C to shut down.");

    if let Err(err) = signal::ctrl_c().await {
        eprintln!("❌ Failed to listen for Ctrl+C: {}", err);
    }
    info!("🛑 Received shutdown signal, exiting.");
}
