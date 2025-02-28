// New Framework (In Development!)

use std::time::Duration;
use dotenv::dotenv;
use server::async_actors::orchestrator::common::RequestedFeed;
use server::async_actors::messages::DummyRequest;
use server::async_actors::orchestrator::orch::Orchestrator;
use tokio;
use tokio::sync::mpsc;
use tokio::time;
use tracing::info;
use tracing_subscriber;

#[tokio::main]
async fn main() {
    
    dotenv().ok();

    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .expect("Failed to install default TLS provider");

    // Logging configuration
    tracing_subscriber::fmt::init();

    // Create a channel for the dummy gRPC requests
    let (dummy_grpc_sender, dummy_grpc_receiver) = mpsc::channel::<DummyRequest>(32);

    // Initialise the orchestrator with the dummy gRPC receiver
    let orchestrator = Orchestrator::new(dummy_grpc_receiver);

    // Spawn the orchestrator's main loop
    tokio::spawn(async move {
        orchestrator.run().await;
    });

    // Allow the orchestrator time to initialise
    time::sleep(time::Duration::from_secs(1)).await;

    // Send the dummy subscribe request (first websocket actor)
    dummy_grpc_sender
        .send(DummyRequest::Subscribe {
            internal_symbol: "Deribit.InvFut.BTC.USD".to_string(),
            exchange: "Deribit".to_string(),
            exchange_symbol: "BTC-PERPETUAL".to_string(),
            requested_feed: RequestedFeed::OrderBook,
        })
        .await
        .expect("Failed to send first subscribe request");

    // Wait to observe heartbeats from the first actor
    time::sleep(Duration::from_secs(5)).await;

    // Send another dummy subscribe request.
    dummy_grpc_sender
        .send(DummyRequest::Subscribe {
            internal_symbol: "Deribit.InvFut.ETH.USD".to_string(),
            exchange: "Deribit".to_string(),
            exchange_symbol: "ETH-PERPETUAL".to_string(),
            requested_feed: RequestedFeed::OrderBook,
        })
        .await
        .expect("Failed to send second subscribe request");

    time::sleep(Duration::from_secs(5)).await;

    // Send a dummy unsubscribe request for the first symbol
    dummy_grpc_sender
        .send(DummyRequest::Unsubscribe {
            internal_symbol: "Deribit.InvFut.BTC.USD".to_string(),
            exchange: "Deribit".to_string(),
            exchange_symbol: "BTC-PERPETUAL".to_string(),
            requested_feed: RequestedFeed::OrderBook,
        })
        .await
        .expect("Failed to send first subscribe request");

    time::sleep(Duration::from_secs(5)).await;

    // Send a dummy unsubscribe request for the second symbol
    dummy_grpc_sender
        .send(DummyRequest::Unsubscribe {
            internal_symbol: "Deribit.InvFut.ETH.USD".to_string(),
            exchange: "Deribit".to_string(),
            exchange_symbol: "ETH-PERPETUAL".to_string(),
            requested_feed: RequestedFeed::OrderBook,
        })
        .await
        .expect("Failed to send second unsubscribe request");

    // Wait to observe heartbeats from the first actor
    time::sleep(Duration::from_secs(5)).await;

    info!("Main function is exiting.");
}
