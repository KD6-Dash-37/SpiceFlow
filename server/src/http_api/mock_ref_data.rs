use crate::async_actors::subscription::{DeribitSubscription, ExchangeSubscription};
use crate::http_api::request_handler::SubscriptionRequest;
use crate::model::{Exchange, RequestedFeed};
use std::fmt;
use std::str::FromStr;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum InstrumentType {
    Spot,
    InvFut,
    LinFut,
}

impl FromStr for InstrumentType {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "spot" => Ok(InstrumentType::Spot),
            "invfut" => Ok(InstrumentType::InvFut),
            "linfut" => Ok(InstrumentType::LinFut),
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
        };
        write!(f, "{}", s)
    }
}

pub fn resolve_subscription(request: &SubscriptionRequest) -> Option<ExchangeSubscription> {
    let exchange = match request.exchange.to_lowercase().as_str() {
        "deribit" => Exchange::Deribit,
        _ => return None,
    };

    let instrument_type = match InstrumentType::from_str(&request.instrument_type) {
        Ok(inst_type) => inst_type,
        Err(_) => return None,
    };

    let internal_symbol = format!(
        "{}.{}.{}.{}",
        exchange, instrument_type, request.base_currency, request.quote_currency,
    );

    let requested_feed = match RequestedFeed::from_str(&request.requested_feed) {
        Ok(inst_type) => inst_type,
        Err(_) => return None,
    };

    let exchange_symbol = match internal_symbol.as_str() {
        "Deribit.InvFut.BTC.USD" => "BTC-PERPETUAL",
        "Deribit.InvFut.ETH.USD" => "ETH-PERPETUAL",
        _ => return None,
    }
    .to_string();

    let sub: ExchangeSubscription = match exchange {
        Exchange::Deribit => {
            DeribitSubscription::new(internal_symbol, exchange_symbol, requested_feed, exchange)
                .into()
        }
    };
    Some(sub)
}
