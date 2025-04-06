// server/src/domain/instrument/deribit.rs

// 📦 External crates
use serde::{Deserialize, Serialize};

// 🧠 Internal modules
use crate::model::InstrumentType;

const KIND_FUTURE: &str = "future";
const KIND_SPOT: &str = "spot";
const TYPE_REVERSED: &str = "reversed";
const TYPE_LINEAR: &str = "linear";
const SETTLEMENT_PERPETUAL: &str = "perpetual";

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct DeribitInstrument {
    pub instrument_name: String,
    pub kind: String,
    pub instrument_type: Option<String>,
    pub settlement_period: Option<String>,
    pub base_currency: String,
    pub quote_currency: String,
}

impl DeribitInstrument {
    pub fn infer_type(&self) -> Option<InstrumentType> {
        match self.kind.as_str() {
            KIND_SPOT => Some(InstrumentType::Spot),
            KIND_FUTURE => {
                match (
                    self.instrument_type.as_deref(),
                    self.settlement_period.as_deref(),
                ) {
                    (Some(TYPE_REVERSED), Some(SETTLEMENT_PERPETUAL)) => {
                        Some(InstrumentType::InvPerp)
                    }
                    (Some(TYPE_LINEAR), Some(SETTLEMENT_PERPETUAL)) => {
                        Some(InstrumentType::LinPerp)
                    }
                    (Some(TYPE_REVERSED), _) => Some(InstrumentType::InvFut),
                    (Some(TYPE_LINEAR), _) => Some(InstrumentType::LinFut),
                    _ => None,
                }
            }
            _ => None,
        }
    }
}
