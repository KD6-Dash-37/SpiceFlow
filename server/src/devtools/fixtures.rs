// server/src/devtools/fixtures.rs
#![cfg(feature = "dev-fixtures")]

// 🌍 Standard library
use std::fs;
use std::path::PathBuf;

// 🧠 Internal modules
use crate::model::InstrumentType;

/// Loads a raw JSON fixture as a string from the `fixtures/` directory.
pub fn load_json_fixture(filename: &str) -> String {
    let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));

    path.pop();
    path.push("fixtures");
    path.push(filename);

    fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("❌ Failed to read fixture file '{}': {}", path.display(), e))
}

/// Loads the appropriate Binance exchangeInfo fixture by instrument type.
///
/// This panics if the file cannot be loaded or parsed — intended for development use only.
pub fn load_binance_exchange_info_fixture(instrument_type: &InstrumentType) -> String {
    let filename = match instrument_type {
        InstrumentType::LinFut | InstrumentType::LinPerp => "exchangeInfoUSDM.json",
        InstrumentType::InvFut | InstrumentType::InvPerp => "exchangeInfoCoinM.json",
        InstrumentType::Spot => "exchangeInfoSpot.json",
    };
    load_json_fixture(filename)
}
