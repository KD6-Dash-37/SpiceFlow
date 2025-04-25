// server/src/domain/sub.rs

// 🧠 Internal modules
use crate::model::{Exchange, RequestedFeed};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExchangeSubscription {
    pub internal_symbol: String,
    pub exchange_symbol: String,
    pub requested_feed: RequestedFeed,
    pub exchange_stream_id: String,
    pub stream_id: String,
    pub exchange: Exchange,
}

impl ExchangeSubscription {
    #[must_use]
    pub fn new(
        internal_symbol: String,
        exchange_symbol: String,
        requested_feed: RequestedFeed,
        exchange: Exchange,
    ) -> Self {
        let exchange_stream_id = match exchange {
            Exchange::Deribit => deribit_stream_id(&exchange_symbol, requested_feed),
            Exchange::Binance => binance_stream_id(&exchange_symbol, requested_feed),
        };
        let stream_id = format!("{internal_symbol}.{requested_feed}");
        Self {
            internal_symbol,
            exchange_symbol,
            requested_feed,
            exchange_stream_id,
            stream_id,
            exchange,
        }
    }
}

fn deribit_stream_id(exchange_symbol: &str, requested_feed: RequestedFeed) -> String {
    match requested_feed {
        RequestedFeed::OrderBook => format!("book.{exchange_symbol}.100ms"),
    }
}

fn binance_stream_id(exchange_symbol: &str, requested_feed: RequestedFeed) -> String {
    match requested_feed {
        RequestedFeed::OrderBook => format!("{}@depth@100ms", exchange_symbol.to_lowercase()),
    }
}
