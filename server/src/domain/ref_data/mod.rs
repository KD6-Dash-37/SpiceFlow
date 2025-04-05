// server/src/domain/ref_data/mod.rs

// 📦 External crates
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use async_trait;

// 🧠 Internal modules
mod deribit;
pub mod types;

use crate::domain::ref_data::types::RefDataError;
use crate::domain::ExchangeSubscription;
use crate::http_api::SubscriptionRequest;
use crate::model::Exchange; // Prefer explicit path here for clarity

use deribit::DeribitRefData;

#[async_trait::async_trait]
pub trait ExchangeRefDataProvider: Send + Sync {
    async fn fetch_instruments(
        &self,
        request: &SubscriptionRequest,
    ) -> Result<Vec<serde_json::Value>, RefDataError>;

    fn match_instrument(
        &self,
        request: &SubscriptionRequest,
        instruments: &[serde_json::Value],
    ) -> Result<Option<serde_json::Value>, RefDataError>;

    fn build_subscription(
        &self,
        request: &SubscriptionRequest,
        instrument: &serde_json::Value,
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
    ) -> Result<Vec<serde_json::Value>, RefDataError> {
        let exchange = Exchange::from_str(&request.exchange)
            .map_err(|_| RefDataError::InvalidExchange(request.exchange.clone()))?;

        let provider = self
            .providers
            .get(&exchange)
            .ok_or(RefDataError::MissingProvider(exchange))?;
        provider.fetch_instruments(request).await
    }

    fn match_instrument(
        &self,
        request: &SubscriptionRequest,
        instruments: &[serde_json::Value],
    ) -> Result<Option<serde_json::Value>, RefDataError> {
        let exchange = Exchange::from_str(&request.exchange)
            .map_err(|_| RefDataError::InvalidExchange(request.exchange.clone()))?;
        let provider = self
            .providers
            .get(&exchange)
            .ok_or(RefDataError::MissingProvider(exchange))?;
        provider.match_instrument(request, instruments)
    }

    fn build_subscription(
        &self,
        request: &SubscriptionRequest,
        instrument: &serde_json::Value,
    ) -> Result<ExchangeSubscription, RefDataError> {
        let exchange = Exchange::from_str(&request.exchange)
            .map_err(|_| RefDataError::InvalidExchange(request.exchange.clone()))?;

        let provider = self
            .providers
            .get(&exchange)
            .ok_or(RefDataError::MissingProvider(exchange))?;
        provider.build_subscription(request, instrument)
    }

    async fn resolve_request(
        &self,
        request: &SubscriptionRequest,
    ) -> Result<ExchangeSubscription, RefDataError> {
        let exchange = Exchange::from_str(&request.exchange)
            .map_err(|_| RefDataError::InvalidExchange(request.exchange.clone()))?;

        let provider = self
            .providers
            .get(&exchange)
            .ok_or(RefDataError::MissingProvider(exchange))?;
        provider.resolve_request(request).await
    }
}

impl RefDataService {
    pub fn with_all_providers() -> Self {
        let mut providers: HashMap<Exchange, Arc<dyn ExchangeRefDataProvider>> = HashMap::new();
        let deribit = Arc::new(DeribitRefData::new());
        providers.insert(Exchange::Deribit, deribit);
        Self { providers }
    }
}
