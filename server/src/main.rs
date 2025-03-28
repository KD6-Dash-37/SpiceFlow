// New Framework (In Development!)
use dotenv::dotenv;
use server::async_actors::orchestrator::Orchestrator;
use server::http_api::handle::SubscriptionAction;
use server::http_api::start_http_server;
use server::http_api::OrchestratorHandle;
use tokio::{signal, sync::mpsc};
use tracing::info;

#[tokio::main]
async fn main() {
    dotenv().ok();

    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .expect("Failed to install default TLS provider");

    // Logging configuration
    tracing_subscriber::fmt::init();

    // Create a channel for the dummy gRPC requests
    // let (dummy_grpc_sender, dummy_grpc_receiver) = mpsc::channel::<DummyRequest>(32);
    let (request_sender, request_receiver) = mpsc::channel::<SubscriptionAction>(32);
    // Initialise the orchestrator with the dummy gRPC receiver
    let orchestrator = Orchestrator::new(request_receiver);

    // Spawn the orchestrator's main loop
    tokio::spawn(async move {
        orchestrator.run().await;
    });

    let http_handle = OrchestratorHandle::new(request_sender);

    tokio::spawn(async move {
        start_http_server(http_handle).await;
    });

    info!("🌱 System started. Press Ctrl+C to shut down.");

    if let Err(err) = signal::ctrl_c().await {
        eprintln!("❌ Failed to listen for Ctrl+C: {}", err);
    }
    info!("🛑 Received shutdown signal, exiting.");
}
