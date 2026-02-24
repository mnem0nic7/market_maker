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

pub mod alpaca;
pub mod binance;
pub mod coinbase;
pub mod ib;
pub mod kraken;
pub mod oms;
pub mod raydium;
pub mod router;
pub mod uniswap;

pub use alpaca::{AlpacaExecutionConfig, AlpacaExecutionConnector};
pub use binance::{BinanceExecutionConfig, BinanceExecutionConnector};
pub use coinbase::{CoinbaseExecutionConfig, CoinbaseExecutionConnector};
pub use ib::{IbExecutionConfig, IbExecutionConnector};
pub use kraken::{KrakenExecutionConfig, KrakenExecutionConnector};
pub use oms::OrderManager;
pub use raydium::{RaydiumExecutionConfig, RaydiumExecutionConnector};
pub use router::OrderRouter;
pub use uniswap::{UniswapExecutionConfig, UniswapExecutionConnector};
