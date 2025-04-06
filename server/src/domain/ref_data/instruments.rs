// server/src/domain/ref_data/instruments.rs

// 📦 External crates
use serde::{Deserialize, Serialize};

#[derive(Debug)]
pub enum ExchangeInstruments {
    Binance(Vec<BinanceSymbol>),
    Deribit(Vec<DeribitInstrument>)
}

impl ExchangeInstruments {
    pub fn as_deribit(&self) -> Option<&Vec<DeribitInstrument>> {
        match self {
            ExchangeInstruments::Deribit(ref instruments) => Some(instruments),
            _ => None,
        }
    }
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

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct DeribitInstrument {
    pub instrument_name: String,
    pub kind: String,
    pub instrument_type: Option<String>,
    pub settlement_period: Option<String>,
    pub base_currency: String,
    pub quote_currency: String,
}