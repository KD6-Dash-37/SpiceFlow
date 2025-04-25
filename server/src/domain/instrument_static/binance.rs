// server/src/domain/ref_data/binance.rs

// 📦 External crates
use async_trait;
use serde::{Deserialize, Serialize};
#[cfg(not(feature = "dev-fixtures"))]
use url::Url;

// 🧠 Internal modules
use super::types::RefDataError;
use super::{ExchangeInstruments, ExchangeRefDataProvider, Instrument};
use crate::domain::ExchangeSubscription;
use crate::http_api::SubscriptionRequest;
use crate::model::{Exchange, InstrumentType};

#[cfg(not(feature = "dev-fixtures"))]
const URL_SPOT: &str = "https://api.binance.com/api/v3/exchangeInfo";
#[cfg(not(feature = "dev-fixtures"))]
const URL_USDM: &str = "https://fapi.binance.com/fapi/v1/exchangeInfo";
#[cfg(not(feature = "dev-fixtures"))]
const URL_COINM: &str = "https://dapi.binance.com/dapi/v1/exchangeInfo";

const CONTRACT_TYPE_PERPETUAL: &str = "PERPETUAL";
const NEXT_Q_FUT: &str = "NEXT_QUARTER";
const CUR_Q_FUT: &str = "CURRENT_QUARTER";

#[allow(clippy::module_name_repetitions)]
#[derive(Debug, Clone, Copy)]
pub enum BinanceInstrumentSource {
    Spot,
    Usdm,
    Coinm,
}

#[allow(clippy::module_name_repetitions)]
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

#[allow(clippy::module_name_repetitions)]
impl BinanceSymbol {
    #[must_use]
    pub fn infer_type(&self, source: BinanceInstrumentSource) -> Option<InstrumentType> {
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
    pub fn to_instrument(&self, source: BinanceInstrumentSource) -> Option<Instrument> {
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

#[allow(clippy::module_name_repetitions)]
#[derive(Debug, Deserialize)]
struct BinanceExchangeInfo {
    symbols: Vec<BinanceSymbol>,
}

#[allow(clippy::module_name_repetitions)]
pub struct BinanceRefData {
    #[allow(dead_code)]
    client: reqwest::Client,
}

impl BinanceRefData {
    pub fn new() -> Self {
        Self {
            client: reqwest::Client::new(),
        }
    }
}

#[async_trait::async_trait]
impl ExchangeRefDataProvider for BinanceRefData {
    async fn fetch_instruments(
        &self,
        request: &SubscriptionRequest,
    ) -> Result<ExchangeInstruments, RefDataError> {
        #[cfg(feature = "dev-fixtures")]
        {
            use crate::devtools;
            let text =
                devtools::fixtures::load_binance_exchange_info_fixture(&request.instrument_type);
            let exchange_info: BinanceExchangeInfo = serde_json::from_str(&text).map_err(|e| {
                RefDataError::ParseError(format!("Failed to parse Binance exchangeInfo: {e}"))
            })?;
            Ok(ExchangeInstruments::Binance(exchange_info.symbols))
        }
        #[cfg(not(feature = "dev-fixtures"))]
        {
            use tracing::{error, info};
            let url = url_for_type(request.instrument_type)?;
            info!("📥 Sending GET request to Binance: {}", url);

            let response = self
                .client
                .get(url)
                .send()
                .await
                .map_err(|e| RefDataError::HttpError(e.to_string()))?;

            if !response.status().is_success() {
                let status = response.status();
                let text = response
                    .text()
                    .await
                    .unwrap_or_else(|_| "<body unreadable>".into());
                error!("❌ Binance returned error status {}: {}", status, text);
                return Err(RefDataError::HttpError(format!(
                    "Binance responded with HTTP {status}: {text}"
                )));
            }

            let text = response.text().await.map_err(|e| {
                RefDataError::HttpError(format!("Failed to read response body: {e}"))
            })?;

            let exchange_info: BinanceExchangeInfo = serde_json::from_str(&text).map_err(|e| {
                RefDataError::ParseError(format!(
                    "Failed to parse Binance exchangeInfo: {e}\nRaw response"
                ))
            })?;
            Ok(ExchangeInstruments::Binance(exchange_info.symbols))
        }
    }

    fn match_instrument(
        &self,
        request: &SubscriptionRequest,
        instruments: &ExchangeInstruments,
    ) -> Result<Option<Instrument>, RefDataError> {
        let ExchangeInstruments::Binance(typed) = instruments else {
            return Err(RefDataError::InstrumentMismatch {
                request: request.to_string(),
                expected: "Binance".into(),
            });
        };

        let source = match request.instrument_type {
            InstrumentType::LinPerp | InstrumentType::LinFut => BinanceInstrumentSource::Usdm,
            InstrumentType::InvPerp | InstrumentType::InvFut => BinanceInstrumentSource::Coinm,
            InstrumentType::Spot => BinanceInstrumentSource::Spot,
        };

        let filtered_instruments = typed
            .iter()
            .filter(|instr| is_binance_match(instr, request, source, request.instrument_type));

        let matched_instruments: Vec<_> = filtered_instruments.collect();

        match matched_instruments.len() {
            0 => Ok(None),
            1 => {
                let instrument = matched_instruments[0]
                    .to_instrument(source)
                    .ok_or_else(|| {
                        RefDataError::ParseError("Failed to serialize BinanceSymbol".into())
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

#[cfg(not(feature = "dev-fixtures"))]
fn url_for_type(instrument_type: InstrumentType) -> Result<Url, RefDataError> {
    let url = match instrument_type {
        InstrumentType::LinPerp | InstrumentType::LinFut => URL_USDM,
        InstrumentType::InvPerp | InstrumentType::InvFut => URL_COINM,
        InstrumentType::Spot => URL_SPOT,
    };
    Url::parse(url).map_err(|_| RefDataError::ParseError("Invalid Binance URL".into()))
}

fn is_binance_match(
    instrument: &BinanceSymbol,
    request: &SubscriptionRequest,
    source: BinanceInstrumentSource,
    expected_type: InstrumentType,
) -> bool {
    instrument.base_currency == request.base
        && instrument.quote_currency == request.quote
        && instrument.infer_type(source) == Some(expected_type)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::http_api::SubscriptionRequest;
    use crate::model::{Exchange, InstrumentType, RequestedFeed};

    fn test_request() -> SubscriptionRequest {
        SubscriptionRequest {
            exchange: Exchange::Binance,
            instrument_type: InstrumentType::LinPerp,
            base: "BTC".into(),
            quote: "USDT".into(),
            requested_feed: RequestedFeed::OrderBook,
        }
    }

    #[tokio::test]
    async fn binance_fetch_instruments_fields_exist() {
        let provider = BinanceRefData::new();
        let request = test_request();

        let instruments = provider
            .fetch_instruments(&request)
            .await
            .expect("Should fetch instruments");

        let ExchangeInstruments::Binance(list) = instruments else {
            panic!("Expected Binance instruments");
        };

        assert!(
            list.iter().any(|instr| {
                !instr.instrument_name.is_empty()
                    && !instr.base_currency.is_empty()
                    && !instr.quote_currency.is_empty()
            }),
            "Expected at least one instrument with required fields"
        );
    }

    #[tokio::test]
    async fn binance_should_match_btc_usdt_perp() {
        let provider = BinanceRefData::new();
        let request = test_request();

        let instruments = provider
            .fetch_instruments(&request)
            .await
            .expect("Instrument fetch failed");

        let matched = provider
            .match_instrument(&request, &instruments)
            .expect("Match logic failed");

        assert!(matched.is_some(), "Expected to match BTCUSDT perpetual");

        let instrument = matched.unwrap();
        assert_eq!(instrument.exchange_symbol, "BTCUSDT");
    }

    #[tokio::test]
    async fn binance_subscription_roundtrip_success() {
        let provider = BinanceRefData::new();
        let request = test_request();

        let subscription = provider
            .resolve_request(&request)
            .await
            .expect("resolve_request should succeed");

        assert_eq!(subscription.exchange_symbol, "BTCUSDT");
        assert!(subscription.stream_id.contains("BTC"));
    }
}
