// server/src/domain/ref_data/mod.rs

// 📦 External crates
use async_trait;
use std::collections::HashMap;
use std::sync::Arc;

// 🧠 Internal modules
mod binance;
mod deribit;
mod types;

use crate::model::{Exchange, InstrumentType};
use crate::domain::ExchangeSubscription;
use crate::http_api::SubscriptionRequest;

use types::RefDataError;
use binance::{BinanceRefData, BinanceSymbol};
use deribit::{DeribitRefData, DeribitInstrument};


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

    #[must_use]
    fn try_from(instr: DeribitInstrument) -> Result<Self, Self::Error> {
        let instrument_type = instr.infer_type().ok_or_else(|| {
            format!(
                "Failed to infer instrument type for DeribitInstrument: {}",
                instr.instrument_name
            )
        })?;
        Ok( Self {
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



#[async_trait::async_trait]
pub trait ExchangeRefDataProvider: Send + Sync {
    async fn fetch_instruments(
        &self,
        request: &SubscriptionRequest,
    ) -> Result<ExchangeInstruments, RefDataError>;

    fn match_instrument(
        &self,
        request: &SubscriptionRequest,
        instruments: &ExchangeInstruments,
    ) -> Result<Option<Instrument>, RefDataError>;

    fn build_subscription(
        &self,
        request: &SubscriptionRequest,
        instrument: &Instrument,
    ) -> Result<ExchangeSubscription, RefDataError>;

    async fn resolve_request(
        &self,
        request: &SubscriptionRequest,
    ) -> Result<ExchangeSubscription, RefDataError>;
}

pub struct RefDataService {
    providers: HashMap<Exchange, Arc<dyn ExchangeRefDataProvider>>,
}

#[async_trait::async_trait]
impl ExchangeRefDataProvider for RefDataService {
    async fn fetch_instruments(
        &self,
        request: &SubscriptionRequest,
    ) -> Result<ExchangeInstruments, RefDataError> {
        let provider = self
            .providers
            .get(&request.exchange)
            .ok_or(RefDataError::MissingProvider(request.exchange))?;
        provider.fetch_instruments(request).await
    }

    fn match_instrument(
        &self,
        request: &SubscriptionRequest,
        instruments: &ExchangeInstruments,
    ) -> Result<Option<Instrument>, RefDataError> {
        let provider = self
            .providers
            .get(&request.exchange)
            .ok_or(RefDataError::MissingProvider(request.exchange))?;
        provider.match_instrument(request, instruments)
    }

    fn build_subscription(
        &self,
        request: &SubscriptionRequest,
        instrument: &Instrument,
    ) -> Result<ExchangeSubscription, RefDataError> {
        let provider = self
            .providers
            .get(&request.exchange)
            .ok_or(RefDataError::MissingProvider(request.exchange))?;
        provider.build_subscription(request, instrument)
    }

    async fn resolve_request(
        &self,
        request: &SubscriptionRequest,
    ) -> Result<ExchangeSubscription, RefDataError> {
        let provider = self
            .providers
            .get(&request.exchange)
            .ok_or(RefDataError::MissingProvider(request.exchange))?;
        provider.resolve_request(request).await
    }
}

impl RefDataService {
    #[must_use]
    pub fn with_all_providers() -> Self {
        let mut providers: HashMap<Exchange, Arc<dyn ExchangeRefDataProvider>> = HashMap::new();

        let deribit = Arc::new(DeribitRefData::new());
        let binance = Arc::new(BinanceRefData::new());

        providers.insert(Exchange::Deribit, deribit);
        providers.insert(Exchange::Binance, binance);

        Self { providers }
    }
}
