//! # mm-execution
//!
//! Order management and exchange execution layer for the market maker platform.
//!
//! This crate provides:
//! - **Order Management System (OMS)** — tracks all orders across exchanges with state
//!   machine validation, indexed by both client and exchange order IDs.
//! - **Smart Order Router** — dispatches order actions to the correct exchange connector
//!   and aggregates order updates back into the OMS.
//! - **Exchange Connectors** — production-quality implementations of `ExecutionConnector`
//!   for Binance, Coinbase, and Kraken, including REST order management and WebSocket
//!   user-data streams for real-time order updates.

pub mod binance;
pub mod coinbase;
pub mod kraken;
pub mod oms;
pub mod router;

pub use binance::{BinanceExecutionConfig, BinanceExecutionConnector};
pub use coinbase::{CoinbaseExecutionConfig, CoinbaseExecutionConnector};
pub use kraken::{KrakenExecutionConfig, KrakenExecutionConnector};
pub use oms::OrderManager;
pub use router::OrderRouter;
