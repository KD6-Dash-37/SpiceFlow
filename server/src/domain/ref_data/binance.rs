// server/src/domain/ref_data/binance.rs

// 🌍 Standard library
use std::str::FromStr;

// 📦 External crates
use async_trait;
use serde::Deserialize;
use tracing::{info, error};
use url::Url;

// 🧠 Internal modules
use super::types::RefDataError;
use super::instruments::{ExchangeInstruments, BinanceSymbol};
use crate::domain::ref_data::ExchangeRefDataProvider;
use crate::domain::{ExchangeSubscription, BinanceSubscription};
use crate::model::{Exchange, InstrumentType, RequestedFeed};
use crate::http_api::SubscriptionRequest;


const URL_SPOT: &str = "https://api.binance.com/api/v3/exchangeInfo";
const URL_USDM: &str = "https://fapi.binance.com/fapi/v1/exchangeInfo"; 
const URL_COINM: &str = "https://dapi.binance.com/dapi/v1/exchangeInfo";

const CONTRACT_TYPE_PERPETUAL: &str = "PERPETUAL";
const NEXT_Q_FUT: &str = "NEXT_QUARTER";
const CUR_Q_FUT: &str = "CURRENT_QUARTER";

#[derive(Debug, Deserialize)]
struct BinanceExchangeInfo {
    symbols: Vec<BinanceSymbol>
}

pub struct BinanceRefData {
    client: reqwest::Client,
}

impl BinanceRefData {
    pub fn new() -> Self {
        Self {
            client: reqwest::Client::new()
        }
    }
}

#[async_trait::async_trait]
impl ExchangeRefDataProvider for BinanceRefData {
    async fn fetch_instruments(
        &self,
        request: &SubscriptionRequest
    ) -> Result<ExchangeInstruments, RefDataError> {
        let instrument_type = InstrumentType::from_str(&request.instrument_type).map_err(|_| {
            RefDataError::InvalidInstrumentType(request.instrument_type.to_string())
        })?;
        
        #[cfg(feature="dev-fixtures")]
        {
            use crate::devtools;
            let text = devtools::fixtures::load_binance_exchange_info_fixture(&instrument_type);
            let exchange_info: BinanceExchangeInfo = serde_json::from_str(&text)
            .map_err(|e| {
                RefDataError::ParseError(format!(
                    "Failed to parse Binance exchangeInfo: {}\nRaw response",
                    e
                ))
            })?;
            Ok(ExchangeInstruments::Binance(exchange_info.symbols))
        }
        #[cfg(not(feature="dev-fixtures"))]
        {
            let url = url_for_type(instrument_type)?;
            info!("📥 Sending GET request to Binance: {}", url);
            
            let response = self.client
                .get(url)
                .send()
                .await
                .map_err(|e| RefDataError::HttpError(e.to_string()))?;
            
            if !response.status().is_success() {
                let status = response.status();
                let text = response.text().await.unwrap_or_else(|_| "<body unreadable>".into());
                error!("❌ Binance returned error status {}: {}", status, text);
                return Err(RefDataError::HttpError(format!(
                    "Binance responded with HTTP {}: {}",
                    status, text
                )));
            }

            let text = response
            .text()
            .await
            .map_err(|e| RefDataError::HttpError(format!("Failed to read response body: {}", e)))?;

            let exchange_info: BinanceExchangeInfo = serde_json::from_str(&text)
                .map_err(|e| {
                    RefDataError::ParseError(format!(
                        "Failed to parse Binance exchangeInfo: {}\nRaw response",
                        e
                    ))
                })?;
                Ok(ExchangeInstruments::Binance(exchange_info.symbols))
        }        
    }

    fn match_instrument(
        &self,
        request: &SubscriptionRequest,
        instruments: &ExchangeInstruments,
    ) -> Result<Option<serde_json::Value>, RefDataError> {
        let ExchangeInstruments::Binance(typed) = instruments else {
            return Err(RefDataError::InstrumentMismatch {
                request: request.to_string(),
                expected: "Binance".into()
            })
        };

        let instrument_type = InstrumentType::from_str(&request.instrument_type)
            .map_err(|_| RefDataError::InvalidInstrumentType(request.instrument_type.clone()))?;

        let filtered_instruments = typed
            .iter()
            .filter(|instr| is_binance_match(instr, request, instrument_type));

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
        instrument: &serde_json::Value
    ) -> Result<ExchangeSubscription, RefDataError> {
        let exchange = Exchange::from_str(&request.exchange)
            .map_err(|_| RefDataError::InvalidExchange(request.exchange.to_string()))?;

        let instrument_type = InstrumentType::from_str(&request.instrument_type).map_err(|_| {
            RefDataError::InvalidInstrumentType(request.instrument_type.to_string())
        })?;

        let requested_feed = RequestedFeed::from_str(&request.requested_feed)
            .map_err(|_| RefDataError::InvalidFeed(request.requested_feed.to_string()))?;

        let exchange_symbol = instrument
            .get("symbol")
            .and_then(|s| s.as_str())
            .ok_or(RefDataError::ParseError(
                "symbol".to_string()
            ))?;

        let internal_symbol = format!(
            "{}.{}.{}.{}",
            exchange,
            instrument_type,
            request.base_currency,
            request.quote_currency
        );
    
        Ok(ExchangeSubscription::from(BinanceSubscription::new(
            internal_symbol,
            exchange_symbol.to_string(),
            requested_feed,
            exchange,
        )))
    }

    async fn resolve_request(
        &self,
        request: &SubscriptionRequest
    ) -> Result<ExchangeSubscription, RefDataError> {
        let instruments = self.fetch_instruments(request).await?;
        let instrument = self
            .match_instrument(request, &instruments)?
            .ok_or_else(|| RefDataError::InstrumentNotFound(request.to_string()))?;
        let subscription = self.build_subscription(request, &instrument)?;
        Ok(subscription)
    }
}


fn url_for_type(instrument_type: InstrumentType) -> Result<Url, RefDataError> {
    let url = match instrument_type {
        InstrumentType::LinPerp | InstrumentType::LinFut => URL_USDM,
        InstrumentType::InvPerp | InstrumentType::InvFut => URL_COINM,
        InstrumentType::Spot => URL_SPOT
    };
    Url::parse(&url).map_err(|_| RefDataError::ParseError("Invalid Binance URL".into()))
}

fn is_binance_match(
    instrument: &BinanceSymbol,
    request: &SubscriptionRequest,
    instrument_type: InstrumentType
) -> bool {
    if instrument.base_currency != request.base_currency || instrument.quote_currency != request.quote_currency {
        return false;
    }
    match instrument_type {
        InstrumentType::LinPerp | InstrumentType::InvPerp  =>  {
            instrument.contract_type.as_deref() == Some(CONTRACT_TYPE_PERPETUAL)
        }
        InstrumentType::LinFut | InstrumentType::InvFut => {
            matches!(
                instrument.contract_type.as_deref(),
                Some(CUR_Q_FUT) | Some(NEXT_Q_FUT)
            )
        }
        InstrumentType::Spot => true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::http_api::SubscriptionRequest;

    #[tokio::test]
    async fn binance_fetch_instruments_fields_exist() {
        let provider = BinanceRefData::new();

        let request = SubscriptionRequest {
            exchange: "Binance".into(),
            instrument_type: "LinPerp".into(),
            base_currency: "BTC".into(),
            quote_currency: "USDT".into(),
            requested_feed: "OrderBook".into(),
        };

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

        let request = SubscriptionRequest {
            exchange: "Binance".into(),
            instrument_type: "LinPerp".into(),
            base_currency: "BTC".into(),
            quote_currency: "USDT".into(),
            requested_feed: "OrderBook".into(),
        };

        let instruments = provider
            .fetch_instruments(&request)
            .await
            .expect("Instrument fetch failed");

        let matched = provider
            .match_instrument(&request, &instruments)
            .expect("Match logic failed");

        assert!(matched.is_some(), "Expected to match BTCUSDT perpetual");

        let instrument = matched.unwrap();
        let exchange_symbol = instrument.get("symbol").unwrap().as_str().unwrap();
        assert_eq!(exchange_symbol, "BTCUSDT");
    }

    #[tokio::test]
    async fn binance_subscription_roundtrip_success() {
        let provider = BinanceRefData::new();

        let request = SubscriptionRequest {
            exchange: "Binance".into(),
            instrument_type: "LinPerp".into(),
            base_currency: "BTC".into(),
            quote_currency: "USDT".into(),
            requested_feed: "OrderBook".into(),
        };

        let subscription = provider
            .resolve_request(&request)
            .await
            .expect("resolve_request should succeed");

        assert_eq!(subscription.exchange_symbol(), "BTCUSDT");
        assert!(subscription.stream_id().contains("BTC"));
    }
}
