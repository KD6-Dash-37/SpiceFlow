// server/src/model/mod.rs

use core::fmt;
use std::str::FromStr;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Exchange {
    Deribit,
    Binance,
}

impl std::str::FromStr for Exchange {
    type Err = &'static str;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "deribit" => Ok(Self::Deribit),
            "binance" => Ok(Self::Binance),
            _ => Err("Unknown or unsupported exchange"),
        }
    }
}

impl std::fmt::Display for Exchange {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Self::Deribit => "Deribit",
            Self::Binance => "Binance",
        };
        write!(f, "{s}")
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
            "orderbook" => Ok(Self::OrderBook),
            _ => Err(()),
        }
    }
}

impl fmt::Display for RequestedFeed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            Self::OrderBook => "OrderBook",
        };
        write!(f, "{s}")
    }
}

impl RequestedFeed {
    #[must_use]
    pub fn from_exchange_str(feed_str: &str) -> Option<Self> {
        match feed_str {
            "book" => Some(Self::OrderBook),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum InstrumentType {
    Spot,
    InvFut,
    LinFut,
    InvPerp,
    LinPerp,
}

impl FromStr for InstrumentType {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "spot" => Ok(Self::Spot),
            "invfut" => Ok(Self::InvFut),
            "linfut" => Ok(Self::LinFut),
            "invperp" => Ok(Self::InvPerp),
            "linperp" => Ok(Self::LinPerp),
            _ => Err(()),
        }
    }
}

impl fmt::Display for InstrumentType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            Self::Spot => "Spot",
            Self::InvFut => "InvFut",
            Self::LinFut => "LinFut",
            Self::InvPerp => "InvPerp",
            Self::LinPerp => "LinPerp",
        };
        write!(f, "{s}")
    }
}
