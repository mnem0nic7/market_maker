//! # mm-market-data
//!
//! Market data ingestion layer for the market-maker platform.
//!
//! This crate provides:
//!
//! - **Exchange connectors** ([`binance`], [`coinbase`], [`kraken`]) that
//!   implement the [`MarketDataConnector`](mm_core::traits::MarketDataConnector)
//!   trait and stream normalized order book and trade data over WebSockets.
//!
//! - **Symbol normalization** ([`normalizer`]) for bidirectional mapping
//!   between canonical [`Symbol`](mm_core::types::Symbol) values and
//!   exchange-specific string representations.
//!
//! - A **local order book** ([`order_book::LocalOrderBook`]) that maintains a
//!   sorted, incrementally-updated order book from WebSocket deltas.
//!
//! - A **market data manager** ([`connector::MarketDataManager`]) that
//!   orchestrates multiple connectors and provides a unified query surface.

pub mod alpaca;
pub mod binance;
pub mod coinbase;
pub mod connector;
pub mod ib;
pub mod kraken;
pub mod normalizer;
pub mod order_book;
pub mod raydium;
pub mod uniswap;

// Re-export the most commonly used types for ergonomic imports.
pub use alpaca::{AlpacaConfig, AlpacaConnector};
pub use binance::{BinanceConnector, BinanceMarketDataConfig};
pub use coinbase::CoinbaseConnector;
pub use connector::MarketDataManager;
pub use ib::{IbConfig, IbConnector};
pub use kraken::KrakenConnector;
pub use normalizer::{from_exchange_symbol, to_exchange_symbol};
pub use order_book::LocalOrderBook;
pub use raydium::{RaydiumConfig, RaydiumConnector};
pub use uniswap::{UniswapConfig, UniswapConnector};
