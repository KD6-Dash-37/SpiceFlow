use std::hash::{Hash, Hasher};

#[derive(Clone, Copy, Debug)]
pub enum RequestedFeed {
    OrderBook,
}

impl RequestedFeed {
    pub fn as_str(self) -> &'static str {
        match self {
            RequestedFeed::OrderBook => "OrderBook",
        }
    }
}

#[derive(Debug)]
pub enum WebSocketActorError {
    SendError(String),
    WriteNotInitialised,
    Timeout,
    ReceiveError(String),
    ConnectionError(String),
}

#[derive(Debug, Clone)]
pub struct Subscription {
    pub internal_symbol: String,
    pub exchange_symbol: String,
    pub requested_feed: RequestedFeed,
}

impl PartialEq for Subscription {
    fn eq(&self, other: &Self) -> bool {
        self.to_stream_id() == other.to_stream_id()
    }
}

impl Eq for Subscription {}

impl Hash for Subscription {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.to_stream_id().hash(state);
    }
}

impl Subscription {
    pub fn new(
        internal_symbol: String,
        exchange_symbol: String,
        requested_feed: RequestedFeed,
    ) -> Self {
        Self {
            internal_symbol,
            exchange_symbol,
            requested_feed,
        }
    }
    pub fn to_stream_id(&self) -> String {
        format!("{}.{}", self.internal_symbol, self.requested_feed.as_str())
    }
}
