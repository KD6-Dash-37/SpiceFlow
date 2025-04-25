// server/src/domain/mod.rs
mod instrument_static;
mod sub;

pub use instrument_static::{ExchangeRefDataProvider, RefDataService};
pub use sub::ExchangeSubscription;
