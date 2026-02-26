//! # mm-core
//!
//! Shared types, traits, events, and errors for the market maker platform.
//! Every crate in the workspace depends on this crate.
//!
//! ## Key Types
//! - [`Order`] — represents a trading order with full lifecycle tracking
//! - [`OrderBook`] — bid/ask price levels for a trading pair
//! - [`Position`] — current holding with entry price and P&L tracking
//! - [`Fill`] — execution report for a completed trade
//! - [`MarketSnapshot`] — point-in-time market state for strategy decisions
//!
//! ## Key Traits
//! - [`traits::MarketDataConnector`] — exchange market data feed adapter
//! - [`traits::ExecutionConnector`] — exchange order execution adapter
//! - [`traits::Strategy`] — trading strategy interface (same for live and backtest)
//! - [`traits::RiskCheck`] — pre-trade risk validation
//! - [`traits::EventPublisher`] — event sourcing publisher abstraction

pub mod errors;
pub mod events;
pub mod metrics;
pub mod strategy_metrics;
pub mod traits;
pub mod types;

pub use errors::*;
pub use events::*;
pub use metrics::MetricsRegistry;
pub use strategy_metrics::{StrategyMetrics, StrategyMetricsTracker};
pub use types::*;
