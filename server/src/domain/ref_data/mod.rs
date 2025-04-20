// server/src/domain/ref_data/mod.rs

// 📦 External crates
use async_trait;
use std::collections::HashMap;
use std::sync::Arc;

// 🧠 Internal modules
mod binance;
mod deribit;
pub mod types;

use crate::domain::instrument::ExchangeInstruments;
use crate::domain::instrument::Instrument;
use crate::domain::ref_data::types::RefDataError;
use crate::domain::ExchangeSubscription;
use crate::http_api::SubscriptionRequest;
use crate::model::Exchange;

use binance::BinanceRefData;
use deribit::DeribitRefData;

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
