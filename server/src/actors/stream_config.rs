// StreamConfig is a mock config which will later be generated dynamically
// by the future StreamManager
use std::fmt;

#[derive(Clone)]
pub enum RequestedFeed {
    OrderBook,
}

impl fmt::Display for RequestedFeed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let feed = match self {
            RequestedFeed::OrderBook => "OrderBook",
        };
        write!(f, "{}", feed)
    }
}

#[derive(Clone)]
pub struct StreamConfig {
    #[allow(dead_code)]
    pub exchange_symbol: &'static str,
    pub internal_symbol: &'static str,
    pub requested_feed: RequestedFeed,
    pub channels: Vec<String>,
}

impl StreamConfig {
    // Static method to return current config
    pub fn current() -> Self {
        Self {
            exchange_symbol: "BTC-PERPETUAL",
            internal_symbol: "Deribit.InvFut.BTC.USD",
            requested_feed: RequestedFeed::OrderBook,
            channels: vec!["book.BTC-PERPETUAL.100ms".to_string()],
        }
    }
}
