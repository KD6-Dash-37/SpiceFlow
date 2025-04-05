// server/src/domain/ref_data/deribit.rs

// 🌍 Standard library
use std::str::FromStr;

// 📦 External crates
use async_trait;
use tracing::debug;
use url::Url;

// 🧠 Internal modules
use super::types::RefDataError;
use crate::domain::ref_data::ExchangeRefDataProvider;
use crate::domain::{DeribitSubscription, ExchangeSubscription};
use crate::http_api::SubscriptionRequest;
use crate::model::{Exchange, InstrumentType, RequestedFeed};

// Deribit string constants
const KIND_FUTURE: &str = "future";
const KIND_SPOT: &str = "spot";
const TYPE_REVERSED: &str = "reversed";
const TYPE_LINEAR: &str = "linear";
const SETTLEMENT_PERPETUAL: &str = "perpetual";

pub struct DeribitRefData {
    client: reqwest::Client,
}

impl DeribitRefData {
    pub fn new() -> Self {
        Self {
            client: reqwest::Client::new(),
        }
    }
}

#[async_trait::async_trait]
impl ExchangeRefDataProvider for DeribitRefData {
    async fn fetch_instruments(
        &self,
        request: &SubscriptionRequest,
    ) -> Result<Vec<serde_json::Value>, RefDataError> {
        let instrument_type = InstrumentType::from_str(&request.instrument_type).map_err(|_| {
            RefDataError::InvalidInstrumentType(request.instrument_type.to_string())
        })?;

        let kind = instrument_type
            .as_exchange_type(&Exchange::Deribit)
            .ok_or(RefDataError::UnsupportedInstrumentType(instrument_type))?;

        let mut url = Url::parse("https://www.deribit.com/api/v2/public/get_instruments")
            .expect("Invalid base URL");

        match instrument_type {
            InstrumentType::Spot | InstrumentType::InvFut | InstrumentType::InvPerp => url
                .query_pairs_mut()
                .append_pair("currency", &request.base_currency),
            InstrumentType::LinFut | InstrumentType::LinPerp => url
                .query_pairs_mut()
                .append_pair("currency", &request.quote_currency),
        };
        url.query_pairs_mut().append_pair("kind", kind);
        debug!("DeribitRefData::fetch_instruments: {}", url);
        let response = self
            .client
            .get(url)
            .send()
            .await
            .map_err(|e| RefDataError::HttpError(e.to_string()))?;

        let body = response
            .json::<serde_json::Value>()
            .await
            .map_err(|e| RefDataError::HttpError(e.to_string()))?;

        let instruments = body
            .get("result")
            .and_then(|v| v.as_array())
            .cloned()
            .ok_or_else(|| RefDataError::ParseError("response.result not found".to_string()))?;
        Ok(instruments)
    }

    fn match_instrument(
        &self,
        request: &SubscriptionRequest,
        instruments: &[serde_json::Value],
    ) -> Result<Option<serde_json::Value>, RefDataError> {
        let instrument_type = InstrumentType::from_str(&request.instrument_type)
            .map_err(|_| RefDataError::InvalidInstrumentType(request.instrument_type.clone()))?;

        let mut matched_instruments = instruments
            .iter()
            .filter(|instr| is_deribit_match(instr, request, instrument_type));

        match (matched_instruments.next(), matched_instruments.next()) {
            (None, _) => Ok(None),
            (Some(first), None) => Ok(Some(first.clone())),
            (Some(_), Some(_)) => {
                let remaining = matched_instruments.count();
                Err(RefDataError::MultipleInstrumentMatches {
                    request: request.to_string(),
                    count: 2 + remaining,
                })
            }
        }
    }

    fn build_subscription(
        &self,
        request: &SubscriptionRequest,
        instrument: &serde_json::Value,
    ) -> Result<ExchangeSubscription, RefDataError> {
        let exchange = Exchange::from_str(&request.exchange)
            .map_err(|_| RefDataError::InvalidExchange(request.exchange.to_string()))?;

        let instrument_type = InstrumentType::from_str(&request.instrument_type).map_err(|_| {
            RefDataError::InvalidInstrumentType(request.instrument_type.to_string())
        })?;

        let requested_feed = RequestedFeed::from_str(&request.requested_feed)
            .map_err(|_| RefDataError::InvalidFeed(request.requested_feed.to_string()))?;

        let internal_symbol = format!(
            "{}.{}.{}.{}",
            exchange, instrument_type, request.base_currency, request.quote_currency,
        );

        let exchange_symbol = instrument
            .get("instrument_name")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .ok_or(RefDataError::ParseError(
                "result[i].instrument_name".to_string(),
            ))?;

        Ok(ExchangeSubscription::from(DeribitSubscription::new(
            internal_symbol,
            exchange_symbol,
            requested_feed,
            exchange,
        )))
    }

    async fn resolve_request(
        &self,
        request: &SubscriptionRequest,
    ) -> Result<ExchangeSubscription, RefDataError> {
        let instruments = self.fetch_instruments(request).await?;
        let instrument = self
            .match_instrument(request, &instruments)?
            .ok_or_else(|| RefDataError::InstrumentNotFound(request.to_string()))?;
        let subscription = self.build_subscription(request, &instrument)?;
        Ok(subscription)
    }
}

fn is_deribit_match(
    instrument: &serde_json::Value,
    request: &SubscriptionRequest,
    instrument_type: InstrumentType,
) -> bool {
    let base = instrument.get("base_currency").and_then(|v| v.as_str());
    let quote = instrument.get("quote_currency").and_then(|v| v.as_str());
    let kind = instrument.get("kind").and_then(|v| v.as_str());
    let instr_type = instrument.get("instrument_type").and_then(|v| v.as_str());
    let settlement_period = instrument.get("settlement_period").and_then(|v| v.as_str());

    let (base, quote, kind, instr_type) = match (base, quote, kind, instr_type) {
        (Some(b), Some(q), Some(k), Some(i)) => (b, q, k, i),
        _ => return false,
    };
    if base != request.base_currency || quote != request.quote_currency {
        return false;
    }

    match instrument_type {
        InstrumentType::Spot => kind == KIND_SPOT,
        InstrumentType::InvPerp => {
            kind == KIND_FUTURE
                && instr_type == TYPE_REVERSED
                && settlement_period == Some(SETTLEMENT_PERPETUAL)
        }
        InstrumentType::LinPerp => {
            kind == KIND_FUTURE
                && instr_type == TYPE_LINEAR
                && settlement_period == Some(SETTLEMENT_PERPETUAL)
        }
        InstrumentType::InvFut => {
            kind == KIND_FUTURE
                && instr_type == TYPE_REVERSED
                && settlement_period != Some(SETTLEMENT_PERPETUAL)
        }
        InstrumentType::LinFut => {
            kind == KIND_FUTURE
                && instr_type == TYPE_LINEAR
                && settlement_period != Some(SETTLEMENT_PERPETUAL)
        }
    }
}

#[tokio::test]
async fn deribit_fetch_instruments_fields_exist() {
    let provider = DeribitRefData::new();

    let request = SubscriptionRequest {
        exchange: "Deribit".into(),
        instrument_type: "InvPerp".into(),
        base_currency: "BTC".into(),
        quote_currency: "USD".into(),
        requested_feed: "OrderBook".into(),
    };

    let instruments = provider
        .fetch_instruments(&request)
        .await
        .expect("Should fetch instruments");

    assert!(
        instruments.iter().any(|instr| {
            instr.get("instrument_name").is_some()
                && instr.get("kind").is_some()
                && instr.get("base_currency").is_some()
                && instr.get("quote_currency").is_some()
                && instr.get("instrument_type").is_some()
        }),
        "Expected at least one instrument with required fields"
    );
}

#[tokio::test]
async fn deribit_should_match_btc_inverse_perp() {
    let provider = DeribitRefData::new();

    let request = SubscriptionRequest {
        exchange: "Deribit".into(),
        instrument_type: "InvPerp".into(),
        base_currency: "BTC".into(),
        quote_currency: "USD".into(),
        requested_feed: "OrderBook".into(),
    };

    let instruments = provider
        .fetch_instruments(&request)
        .await
        .expect("Instrument fetch failed");

    let matched = provider
        .match_instrument(&request, &instruments)
        .expect("Match logic failed");

    assert!(
        matched.is_some(),
        "Expected to match BTC-PERPETUAL but got None"
    );

    let instrument = matched.unwrap();
    let exchange_symbol = instrument.get("instrument_name").unwrap().as_str().unwrap();
    assert_eq!(exchange_symbol, "BTC-PERPETUAL");
}

#[tokio::test]
async fn deribit_subscription_roundtrip_success() {
    let provider = DeribitRefData::new();

    let request = SubscriptionRequest {
        exchange: "Deribit".into(),
        instrument_type: "InvPerp".into(),
        base_currency: "BTC".into(),
        quote_currency: "USD".into(),
        requested_feed: "OrderBook".into(),
    };

    let subscription = provider
        .resolve_request(&request)
        .await
        .expect("resolve_request should succeed");

    assert_eq!(subscription.exchange_symbol(), "BTC-PERPETUAL");
    assert!(subscription.stream_id().contains("BTC"));
}
