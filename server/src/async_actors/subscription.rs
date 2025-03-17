use crate::async_actors::messages::Exchange;
use crate::async_actors::orchestrator::common::RequestedFeed;
use std::hash::{Hash, Hasher};

pub trait SubscriptionInfo {
    fn exchange_symbol(&self) -> &str;
    fn exchange_stream_id(&self) -> &str;
    fn feed_type(&self) -> RequestedFeed;
    fn exchange(&self) -> Exchange;
}

#[derive(Debug, Clone)]
pub enum ExchangeSubscription {
    Deribit(DeribitSubscription),
}

impl PartialEq for ExchangeSubscription {
    fn eq(&self, other: &Self) -> bool {
        self.stream_id() == other.stream_id()
    }
}

impl Eq for ExchangeSubscription {}

impl Hash for ExchangeSubscription {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.stream_id().hash(state);
    }
}

impl ExchangeSubscription {
    pub fn stream_id(&self) -> &str {
        match self {
            ExchangeSubscription::Deribit(sub) => &sub.stream_id,
        }
    }
}

impl SubscriptionInfo for ExchangeSubscription {
    fn exchange_symbol(&self) -> &str {
        match self {
            ExchangeSubscription::Deribit(sub) => sub.exchange_symbol(),
        }
    }
    fn feed_type(&self) -> RequestedFeed {
        match self {
            ExchangeSubscription::Deribit(sub) => sub.feed_type(),
        }
    }
    fn exchange_stream_id(&self) -> &str {
        match self {
            ExchangeSubscription::Deribit(sub) => sub.exchange_stream_id(),
        }
    }
    fn exchange(&self) -> Exchange {
        match self {
            ExchangeSubscription::Deribit(sub) => sub.exchange(),
        }
    }
}

impl SubscriptionInfo for DeribitSubscription {
    fn exchange_symbol(&self) -> &str {
        &self.exchange_symbol
    }
    fn feed_type(&self) -> RequestedFeed {
        self.requested_feed
    }
    fn exchange_stream_id(&self) -> &str {
        &self.exchange_stream_id
    }
    fn exchange(&self) -> Exchange {
        self.exchange
    }
}

#[derive(Debug, Clone)]
pub struct DeribitSubscription {
    pub internal_symbol: String,
    pub exchange_symbol: String,
    pub requested_feed: RequestedFeed,
    pub stream_id: String,
    pub exchange_stream_id: String,
    pub exchange: Exchange,
}

impl PartialEq for DeribitSubscription {
    fn eq(&self, other: &Self) -> bool {
        self.stream_id == other.stream_id
    }
}

impl Eq for DeribitSubscription {}

impl Hash for DeribitSubscription {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.stream_id.hash(state);
    }
}

impl DeribitSubscription {
    pub fn new(
        internal_symbol: String,
        exchange_symbol: String,
        requested_feed: RequestedFeed,
        exchange: Exchange,
    ) -> Self {
        let stream_id = format!("{}.{}", internal_symbol, requested_feed.as_str());
        let exchange_stream_id = match requested_feed {
            RequestedFeed::OrderBook => {
                format!("book.{}.100ms", &exchange_symbol)
            }
        };
        Self {
            internal_symbol,
            exchange_symbol,
            requested_feed,
            stream_id,
            exchange_stream_id,
            exchange,
        }
    }
}
