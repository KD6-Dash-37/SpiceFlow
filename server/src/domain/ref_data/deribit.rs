// server/src/domain/ref_data/deribit.rs

// 📦 External crates
use async_trait;
use serde::Deserialize;
use tracing::debug;
use url::Url;

// 🧠 Internal modules
use super::types::RefDataError;
use crate::domain::instrument::{DeribitInstrument, ExchangeInstruments, Instrument};
use crate::domain::ref_data::ExchangeRefDataProvider;
use crate::domain::ExchangeSubscription;
use crate::http_api::SubscriptionRequest;
use crate::model::InstrumentType;

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
        let kind = instrument_type_to_deribit_kind(&request.instrument_type);

        let mut url = Url::parse("https://www.deribit.com/api/v2/public/get_instruments")
            .expect("Invalid base URL");

        match request.instrument_type {
            InstrumentType::Spot | InstrumentType::InvFut | InstrumentType::InvPerp => {
                url.query_pairs_mut().append_pair("currency", &request.base)
            }
            InstrumentType::LinFut | InstrumentType::LinPerp => url
                .query_pairs_mut()
                .append_pair("currency", &request.quote),
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
    ) -> Result<Option<Instrument>, RefDataError> {
        let ExchangeInstruments::Deribit(typed) = instruments else {
            return Err(RefDataError::InstrumentMismatch {
                request: request.to_string(),
                expected: "Deribit".into(),
            });
        };

        let filtered_instruments = typed.iter().filter(|instr| {
            instr.base_currency == request.base
                && instr.quote_currency == request.quote
                && instr.infer_type() == Some(request.instrument_type)
        });

        let matched_instruments: Vec<_> = filtered_instruments.collect();

        match matched_instruments.len() {
            0 => Ok(None),
            1 => {
                let instrument = matched_instruments[0].clone().try_into().map_err(|e| {
                    RefDataError::ParseError(format!(
                        "Failed to convert {request} to Instrument: {e}"
                    ))
                })?;
                Ok(Some(instrument))
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
        instrument: &Instrument,
    ) -> Result<ExchangeSubscription, RefDataError> {
        let internal_symbol = instrument.to_internal_symbol();
        let exchange_symbol = instrument.exchange_symbol.clone();

        Ok(ExchangeSubscription::new(
            internal_symbol,
            exchange_symbol,
            request.requested_feed,
            instrument.exchange,
        ))
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

const fn instrument_type_to_deribit_kind(instrument_type: &InstrumentType) -> &'static str {
    match instrument_type {
        InstrumentType::InvFut
        | InstrumentType::LinFut
        | InstrumentType::InvPerp
        | InstrumentType::LinPerp => "future",
        InstrumentType::Spot => "spot",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::http_api::SubscriptionRequest;
    use crate::model::{Exchange, InstrumentType, RequestedFeed};

    fn test_request() -> SubscriptionRequest {
        SubscriptionRequest {
            exchange: Exchange::Deribit,
            instrument_type: InstrumentType::InvPerp,
            base: "BTC".into(),
            quote: "USD".into(),
            requested_feed: RequestedFeed::OrderBook,
        }
    }

    #[tokio::test]
    async fn fetch_instruments_fields_exist() {
        let provider = DeribitRefData::new();
        let request = test_request();

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
    async fn should_match_btc_inverse_perp() {
        let provider = DeribitRefData::new();
        let request = test_request();

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
        assert_eq!(instrument.exchange_symbol, "BTC-PERPETUAL");
    }

    #[tokio::test]
    async fn subscription_roundtrip_success() {
        let provider = DeribitRefData::new();
        let request = test_request();

        let subscription = provider
            .resolve_request(&request)
            .await
            .expect("resolve_request should succeed");

        assert_eq!(subscription.exchange_symbol, "BTC-PERPETUAL");
        assert!(subscription.stream_id.contains("BTC"));
    }
}
