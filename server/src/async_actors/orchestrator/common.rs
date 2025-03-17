// async_actors/orchestrator/common.rs

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RequestedFeed {
    OrderBook,
}

impl RequestedFeed {
    pub fn as_str(self) -> &'static str {
        match self {
            RequestedFeed::OrderBook => "OrderBook",
        }
    }

    pub fn from_exchange_str(feed_str: &str) -> Option<Self> {
        match feed_str {
            "book" => Some(RequestedFeed::OrderBook),
            _ => None,
        }
    }
}
