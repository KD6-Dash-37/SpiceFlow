// server/src/model/mod.rs

use core::fmt;
use std::str::FromStr;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Exchange {
    Deribit,
}

impl std::str::FromStr for Exchange {
    type Err = &'static str;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "deribit" => Ok(Exchange::Deribit),
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
            "spot" => Ok(InstrumentType::Spot),
            "invfut" => Ok(InstrumentType::InvFut),
            "linfut" => Ok(InstrumentType::LinFut),
            "invperp" => Ok(InstrumentType::InvPerp),
            "linperp" => Ok(InstrumentType::LinPerp),
            _ => Err(()),
        }
    }
}

impl fmt::Display for InstrumentType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            InstrumentType::Spot => "Spot",
            InstrumentType::InvFut => "InvFut",
            InstrumentType::LinFut => "LinFut",
            InstrumentType::InvPerp => "InvPerp",
            InstrumentType::LinPerp => "LinPerp",
        };
        write!(f, "{}", s)
    }
}

impl InstrumentType {
    pub fn as_exchange_type(&self, exchange: &Exchange) -> Option<&'static str> {
        match exchange {
            Exchange::Deribit => match self {
                InstrumentType::InvFut
                | InstrumentType::LinFut
                | InstrumentType::InvPerp
                | InstrumentType::LinPerp => Some("future"),
                InstrumentType::Spot => Some("spot"),
            },
        }
    }
}
