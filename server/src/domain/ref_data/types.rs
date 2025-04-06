// server/src/domain/ref_data/types.rs

// 🧠 Internal modules
use crate::model::{Exchange, InstrumentType};

#[derive(Debug, Clone)]
pub struct Instrument {
    pub internal_symbol: String,
    pub exchange_symbol: String,
    // add more fields as needed (e.g. tick_size, contract_size, etc.)
}

#[derive(Debug, thiserror::Error)]
pub enum RefDataError {
    #[error("Failed to fetch ref data from exchange")]
    FetchFailed,
    #[error("Parse error: {0}")]
    ParseError(String),
    #[error("Unsupported instrument type: {0}")]
    UnsupportedInstrumentType(InstrumentType),
    #[error("Invalid instrument type: {0}")]
    InvalidInstrumentType(String),
    #[error("HttpError: {0}")]
    HttpError(String),
    #[error("Venue not supported: {0}")]
    InvalidExchange(String),
    #[error("RefData provider not found: {0}")]
    MissingProvider(Exchange),
    #[error("SubscriptionRequest: {request} matched multiple instruments: {count} matches!")]
    MultipleInstrumentMatches { request: String, count: usize },
    #[error("Invalid feed: {0}")]
    InvalidFeed(String),
    #[error("Instrument not found for request: {0}")]
    InstrumentNotFound(String),
    #[error("Request: {request}, expected: {expected}")]
    InstrumentMismatch { request: String, expected: String}
}

#[derive(Debug, thiserror::Error)]
pub enum SubscriptionError {
    #[error("Unknown exchange")]
    UnknownExchange,
    #[error("Invalid instrument type: {0}")]
    InvalidInstrumentType(String),
    #[error("Invalid feed")]
    InvalidFeed,
    #[error("Instrument not found")]
    InstrumentNotFound,
}
