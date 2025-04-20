// server/src/http_api/requests.rs

// 🌍 Standard library
use std::convert::TryFrom;
use std::fmt;
use std::str::FromStr;

// 📦 External crates
use serde::Deserialize;
use thiserror::Error;

// 🧠 Internal modules
use crate::model::{Exchange, InstrumentType, RequestedFeed};

#[derive(Debug, Error)]
pub enum SubscriptionError {
    #[error("Invalid or unsupported instrument type: {0}")]
    InstrumentType(String),
    #[error("Invalid or unsupported exchange: {0}")]
    Exchange(String),
    #[error("Invalid or unsupported requested feed: {0}")]
    Feed(String),
}

#[derive(Clone, Debug, Deserialize)]
pub struct RawSubscriptionRequest {
    base: String,
    quote: String,
    instrument_type: String,
    exchange: String,
    requested_feed: String,
}

pub struct SubscriptionRequest {
    pub base: String,
    pub quote: String,
    pub instrument_type: InstrumentType,
    pub exchange: Exchange,
    pub requested_feed: RequestedFeed,
}

impl TryFrom<RawSubscriptionRequest> for SubscriptionRequest {
    type Error = SubscriptionError;

    fn try_from(req: RawSubscriptionRequest) -> Result<Self, Self::Error> {
        let instrument_type = InstrumentType::from_str(&req.instrument_type)
            .map_err(|()| SubscriptionError::InstrumentType(req.instrument_type))?;

        let exchange = Exchange::from_str(&req.exchange)
            .map_err(|_| SubscriptionError::Exchange(req.exchange))?;

        let requested_feed = RequestedFeed::from_str(&req.requested_feed)
            .map_err(|()| SubscriptionError::Feed(req.requested_feed))?;

        Ok(Self {
            base: req.base.to_uppercase(),
            quote: req.quote.to_uppercase(),
            instrument_type,
            exchange,
            requested_feed,
        })
    }
}

impl fmt::Display for SubscriptionRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}.{}.{}.{} -> {}",
            self.exchange, self.instrument_type, self.base, self.quote, self.requested_feed
        )
    }
}
