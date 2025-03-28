use core::fmt;
use std::str::FromStr;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Exchange {
    Deribit,
}

impl std::str::FromStr for Exchange {
    type Err = &'static str;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "Deribit" => Ok(Exchange::Deribit),
            _ => Err("Unknown or unsupported exchange"),
        }
    }
}

impl std::fmt::Display for Exchange {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Exchange::Deribit => "Deribit",
        };
        write!(f, "{}", s)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RequestedFeed {
    OrderBook,
}

impl FromStr for RequestedFeed {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "orderbook" => Ok(RequestedFeed::OrderBook),
            _ => Err(()),
        }
    }
}

impl fmt::Display for RequestedFeed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            RequestedFeed::OrderBook => "OrderBook",
        };
        write!(f, "{}", s)
    }
}

impl RequestedFeed {
    pub fn from_exchange_str(feed_str: &str) -> Option<Self> {
        match feed_str {
            "book" => Some(RequestedFeed::OrderBook),
            _ => None,
        }
    }
}
