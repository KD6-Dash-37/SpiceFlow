// server/src/devtools/fixtures.rs
#![cfg(feature = "dev-fixtures")]

// 🌍 Standard library
use std::fs;
use std::path::PathBuf;

// 🧠 Internal modules
use crate::model::InstrumentType;

/// Loads a raw JSON fixture as a string from the `fixtures/` directory.
///
/// # Arguments
/// * `filename` - The name of the fixture file to load.
///
/// # Returns
/// The contents of the fixture file as a `String`.
///
/// # Panics
/// Panics if the file cannot be read. Intended for development use only.
#[must_use]
pub fn load_json_fixture(filename: &str) -> String {
    let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));

    path.pop();
    path.push("fixtures");
    path.push(filename);

    fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("❌ Failed to read fixture file '{}': {}", path.display(), e))
}

/// Loads the appropriate Binance `exchangeInfo` fixture by instrument type.
///
/// # Arguments
/// * `instrument_type` - The type of instrument to load the fixture for.
///
/// # Returns
/// The contents of the corresponding fixture file as a `String`.
///
/// # Panics
/// Panics if the fixture file cannot be found or parsed. Intended for development use only.
#[must_use]
pub fn load_binance_exchange_info_fixture(instrument_type: &InstrumentType) -> String {
    let filename = match instrument_type {
        InstrumentType::LinFut | InstrumentType::LinPerp => "exchangeInfoUSDM.json",
        InstrumentType::InvFut | InstrumentType::InvPerp => "exchangeInfoCoinM.json",
        InstrumentType::Spot => "exchangeInfoSpot.json",
    };
    load_json_fixture(filename)
}
