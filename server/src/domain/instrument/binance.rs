// server/src/domain/instrument/binance.rs

// 📦 External crates
use serde::{Deserialize, Serialize};

// 🧠 Internal modules
use super::Instrument;
use crate::model::{Exchange, InstrumentType};

const CONTRACT_TYPE_PERPETUAL: &str = "PERPETUAL";
const NEXT_Q_FUT: &str = "NEXT_QUARTER";
const CUR_Q_FUT: &str = "CURRENT_QUARTER";

#[derive(Debug, Clone, Copy)]
pub enum BinanceInstrumentSource {
    Spot,
    Usdm,
    Coinm,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct BinanceSymbol {
    #[serde(rename = "symbol")]
    pub instrument_name: String,
    #[serde(rename = "baseAsset")]
    pub base_currency: String,
    #[serde(rename = "quoteAsset")]
    pub quote_currency: String,
    #[serde(rename = "contractType")]
    pub contract_type: Option<String>,
}

impl BinanceSymbol {
    #[must_use]
    pub fn infer_type(&self, source: &BinanceInstrumentSource) -> Option<InstrumentType> {
        match source {
            BinanceInstrumentSource::Spot => Some(InstrumentType::Spot),
            BinanceInstrumentSource::Usdm => match self.contract_type.as_deref()? {
                CONTRACT_TYPE_PERPETUAL => Some(InstrumentType::LinPerp),
                CUR_Q_FUT | NEXT_Q_FUT => Some(InstrumentType::LinFut),
                _ => None,
            },
            BinanceInstrumentSource::Coinm => match self.contract_type.as_deref()? {
                CONTRACT_TYPE_PERPETUAL => Some(InstrumentType::InvPerp),
                CUR_Q_FUT | NEXT_Q_FUT => Some(InstrumentType::InvFut),
                _ => None,
            },
        }
    }

    #[must_use]
    pub fn to_instrument(&self, source: &BinanceInstrumentSource) -> Option<Instrument> {
        let instrument_type = self.infer_type(source)?;
        Some(Instrument {
            exchange: Exchange::Binance,
            exchange_symbol: self.instrument_name.clone(),
            base: self.base_currency.clone(),
            quote: self.quote_currency.clone(),
            instrument_type,
        })
    }
}
