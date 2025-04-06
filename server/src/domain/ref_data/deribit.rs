// server/src/domain/ref_data/deribit.rs

// 🌍 Standard library
use std::str::FromStr;

// 📦 External crates
use async_trait;
use serde::Deserialize;
use tracing::debug;
use url::Url;

// 🧠 Internal modules
use super::types::RefDataError;
use super::instruments::{DeribitInstrument, ExchangeInstruments};
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


#[derive(Debug, Deserialize)]
struct DeribitGetInstrumentsResponse {
    result: Vec<DeribitInstrument>,
}

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
    ) -> Result<ExchangeInstruments, RefDataError> {
        let instrument_type = InstrumentType::from_str(&request.instrument_type).map_err(|_| {
            RefDataError::InvalidInstrumentType(request.instrument_type.to_string())
        })?;

        let kind = instrument_type_to_deribit_kind(&instrument_type);

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

        let data = response
            .json::<DeribitGetInstrumentsResponse>()
            .await
            .map_err(|e| RefDataError::HttpError(e.to_string()))?;

        Ok(ExchangeInstruments::Deribit(data.result))
    }

    fn match_instrument(
        &self,
        request: &SubscriptionRequest,
        instruments: &ExchangeInstruments,
    ) -> Result<Option<serde_json::Value>, RefDataError> {
        let ExchangeInstruments::Deribit(typed) = instruments else {
            return Err(RefDataError::InstrumentMismatch {
                request: request.to_string(),
                expected: "Deribit".into()
            });
        };
        
        let instrument_type = InstrumentType::from_str(&request.instrument_type)
            .map_err(|_| RefDataError::InvalidInstrumentType(request.instrument_type.clone()))?;

        let filtered_instruments = typed
            .iter()
            .filter(|instr| is_deribit_match(instr, request, instrument_type));
        
        let matched_instruments: Vec<_> = filtered_instruments.collect();
        
        match matched_instruments.len() {
            0 => Ok(None),
            1 => {
                let value = serde_json::to_value(matched_instruments[0])
                    .map_err(|_| RefDataError::ParseError("Failed to serialize BinanceSymbol".into()))?;
                Ok(Some(value))
            }
            n => Err(RefDataError::MultipleInstrumentMatches {
                request: request.to_string(),
                count: n,
            }),
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

fn instrument_type_to_deribit_kind(instrument_type: &InstrumentType) -> &'static str {
    match instrument_type {
        InstrumentType::InvFut
        | InstrumentType::LinFut
        | InstrumentType::InvPerp
        | InstrumentType::LinPerp => "future",
        InstrumentType::Spot => "spot"
    }
}


fn is_deribit_match(
    instrument: &DeribitInstrument,
    request: &SubscriptionRequest,
    instrument_type: InstrumentType,
) -> bool {
    if instrument.base_currency != request.base_currency || instrument.quote_currency != request.quote_currency {
        return false;
    }

    match instrument_type {
        InstrumentType::Spot => instrument.kind == KIND_SPOT,
        InstrumentType::InvPerp => {
            instrument.kind == KIND_FUTURE &&
            instrument.instrument_type == Some(TYPE_REVERSED.to_string()) &&
            instrument.settlement_period.as_deref() == Some(SETTLEMENT_PERPETUAL)
        }
        InstrumentType::LinPerp => {
            instrument.kind == KIND_FUTURE &&
            instrument.instrument_type == Some(TYPE_LINEAR.to_string()) &&
            instrument.settlement_period.as_deref() == Some(SETTLEMENT_PERPETUAL)
        }
        InstrumentType::InvFut => {
            instrument.kind == KIND_FUTURE &&
            instrument.instrument_type == Some(TYPE_REVERSED.to_string()) &&
            instrument.settlement_period.as_deref() != Some(SETTLEMENT_PERPETUAL)
        }
        InstrumentType::LinFut => {
            instrument.kind == KIND_FUTURE &&
            instrument.instrument_type == Some(TYPE_LINEAR.to_string()) &&
            instrument.settlement_period.as_deref() != Some(SETTLEMENT_PERPETUAL)
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

    let ExchangeInstruments::Deribit(list) = instruments else {
        panic!("Expected Deribit instruments");
    };

    assert!(
        list.iter().any(|instr| {
            !instr.instrument_name.is_empty()
                && !instr.kind.is_empty()
                && !instr.base_currency.is_empty()
                && !instr.quote_currency.is_empty()
                && instr.instrument_type.is_some()
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
    let exchange_symbol = instrument
        .get("instrument_name")
        .unwrap()
        .as_str()
        .unwrap();
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

