// server/src/domain/subscription/mod.rs

use crate::model::{Exchange, RequestedFeed};

pub type ExchangeSubscription = Box<dyn SubscriptionInfo>;

impl From<DeribitSubscription> for ExchangeSubscription {
    fn from(sub: DeribitSubscription) -> Self {
        Box::new(sub)
    }
}

pub trait SubscriptionInfo: std::fmt::Debug + Send + Sync {
    fn stream_id(&self) -> &str;
    fn exchange_symbol(&self) -> &str;
    fn exchange_stream_id(&self) -> &str;
    fn feed_type(&self) -> RequestedFeed;
    fn exchange(&self) -> Exchange;
    fn clone_box(&self) -> Box<dyn SubscriptionInfo>;
}

impl Clone for Box<dyn SubscriptionInfo> {
    fn clone(&self) -> Self {
        self.clone_box()
    }
}

#[derive(Debug)]
pub struct DeribitSubscription {
    pub internal_symbol: String,
    pub exchange_symbol: String,
    pub requested_feed: RequestedFeed,
    pub stream_id: String,
    pub exchange_stream_id: String,
    pub exchange: Exchange,
}

impl Clone for DeribitSubscription {
    fn clone(&self) -> Self {
        Self {
            internal_symbol: self.internal_symbol.clone(),
            exchange_symbol: self.exchange_symbol.clone(),
            requested_feed: self.requested_feed,
            stream_id: self.stream_id.clone(),
            exchange_stream_id: self.exchange_stream_id.clone(),
            exchange: self.exchange,
        }
    }
}

impl SubscriptionInfo for DeribitSubscription {
    fn stream_id(&self) -> &str {
        &self.stream_id
    }
    fn exchange_symbol(&self) -> &str {
        &self.exchange_symbol
    }
    fn exchange_stream_id(&self) -> &str {
        &self.exchange_stream_id
    }
    fn feed_type(&self) -> RequestedFeed {
        self.requested_feed
    }
    fn exchange(&self) -> Exchange {
        self.exchange
    }
    fn clone_box(&self) -> Box<dyn SubscriptionInfo> {
        Box::new(self.clone())
    }
}

impl DeribitSubscription {
    pub fn new(
        internal_symbol: String,
        exchange_symbol: String,
        requested_feed: RequestedFeed,
        exchange: Exchange,
    ) -> Self {
        let stream_id = format!("{}.{}", internal_symbol, requested_feed);
        let exchange_stream_id = match requested_feed {
            RequestedFeed::OrderBook => format!("book.{}.100ms", &exchange_symbol),
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
