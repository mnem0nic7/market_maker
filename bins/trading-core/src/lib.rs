//! Library face of the trading-core binary.
//!
//! Re-exports the configuration module so that integration tests in the
//! workspace root can construct and validate [`TradingConfig`] instances.

pub mod config;
