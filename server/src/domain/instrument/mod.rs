// server/src/domain/instrument/mod.rs

// 🌍 Standard library
use std::convert::TryFrom;

// 🧠 Internal modules
use crate::model::{Exchange, InstrumentType};

mod binance;
mod deribit;
pub use binance::{BinanceInstrumentSource, BinanceSymbol};
pub use deribit::DeribitInstrument;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Instrument {
    pub exchange: Exchange,
    pub exchange_symbol: String,
    pub base: String,
    pub quote: String,
    pub instrument_type: InstrumentType,
}

impl TryFrom<DeribitInstrument> for Instrument {
    type Error = String;

    fn try_from(instr: DeribitInstrument) -> Result<Self, Self::Error> {
        let instrument_type = instr.infer_type().ok_or_else(|| {
            format!(
                "Failed to infer instrument type for DeribitInstrument: {}",
                instr.instrument_name
            )
        })?;
        Ok(Instrument {
            exchange: Exchange::Deribit,
            exchange_symbol: instr.instrument_name,
            base: instr.base_currency,
            quote: instr.quote_currency,
            instrument_type,
        })
    }
}

impl Instrument {
    pub fn to_internal_symbol(&self) -> String {
        format!(
            "{}.{}.{}.{}",
            self.exchange, self.instrument_type, self.base, self.quote
        )
    }
}

#[derive(Debug)]
pub enum ExchangeInstruments {
    Binance(Vec<BinanceSymbol>),
    Deribit(Vec<DeribitInstrument>),
}

impl ExchangeInstruments {
    pub fn as_deribit(&self) -> Option<&Vec<DeribitInstrument>> {
        match self {
            ExchangeInstruments::Deribit(ref instruments) => Some(instruments),
            _ => None,
        }
    }
}
