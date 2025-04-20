// server/src/async_actors/orchestrator/errors.rs

// 📦 External crates
use thiserror::Error;

use crate::model::Exchange;

#[derive(Debug, Error)]
pub enum OrchestratorError {
    // -------------------------------------------------------
    // Timeout errors
    // -------------------------------------------------------
    #[error("❌ RouterActor {actor_id} timed out")]
    RouterActorTimeout { actor_id: String },
    #[error("❌ WebSocketActor {actor_id} timed out")]
    WebSocketActorTimeout { actor_id: String },
    #[error("❌ OrderBookActor {actor_id} timed out")]
    OrderBookActorTimeout { actor_id: String },
    #[error("❌ BroastcastActor {actor_id} timed out")]
    BroastcastActorTimeout { actor_id: String },

    // -------------------------------------------------------
    // Channel errors
    // -------------------------------------------------------
    #[error("❌ RouterActor {actor_id} ws<>router channel is closed")]
    RouterActorChannelClosed { actor_id: String },
    #[error("Could not find the raw_market_data_sender for {stream_id} while subscribing RouterActor {router_actor_id}")]
    RawDataChannelMissing {
        stream_id: String,
        router_actor_id: String,
    },
    #[error("❌ Channel closed")]
    ChannelClosed,

    // -------------------------------------------------------
    // Cannot find actor by type
    // -------------------------------------------------------
    #[error("❌ RouterActor not found")]
    RouterActorMissing,
    #[error("❌ WebSocketActor not found")]
    WebSocketActorMissing,
    #[error("❌ OrderBookActor not found")]
    OrderBookActorMissing,
    #[error("❌ BroadcastActor not found")]
    BroadcastActorMissing,

    // -------------------------------------------------------
    // Cannot find actor by ID
    // -------------------------------------------------------
    #[error("❌ WebSocketActor: {actor_id} not found")]
    WebSocketNotFound { actor_id: String },
    #[error("❌ RouterActor: {actor_id} not found")]
    RouterNotFound { actor_id: String },
    #[error("❌ OrderBookActor: {actor_id} not found")]
    OrderBookNotFound { actor_id: String },
    #[error("❌ BroastcastActor: {actor_id} not found")]
    BroastcastActorNotFound { actor_id: String },

    // -------------------------------------------------------
    // Subscription errors
    // -------------------------------------------------------
    #[error("❌ Subscription: {stream_id} not found")]
    ExistingSubscriptionNotFound { stream_id: String },
    #[error("❌ UnsupportedFeedType: {feed_type}")]
    UnsupportedFeedType { feed_type: String },
    #[error("❌ Unsupportedexchange: {exchange}")]
    UnsupportedExchange {exchange: Exchange},

    #[error("❌ {reason}")]
    TaskLogicError { reason: String },
}
