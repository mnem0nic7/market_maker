//! # mm-data-pipeline
//!
//! Durable event streaming, time-series persistence, and tamper-evident audit
//! trail for the market-maker platform.
//!
//! ## Modules
//!
//! * [`event_store`] -- NATS JetStream publisher and subscriber.
//! * [`timescale`]   -- TimescaleDB persistence (market data, trades, orders,
//!   fills, positions, events).
//! * [`audit`]       -- Hash-chained audit logger (NATS -> DB + JSONL file).
//! * [`errors`]      -- Crate-level error types.

pub mod audit;
pub mod errors;
pub mod event_store;
pub mod timescale;

// Re-export the most commonly used types at the crate root.
pub use audit::{AuditLogger, AuditStats};
pub use errors::PipelineError;
pub use event_store::{NatsConfig, NatsEventPublisher, NatsEventSubscriber};
pub use timescale::{
    EventRow, FillRow, MarketTickRow, OrderRow, PositionRow, TimescaleStore, TradeRow,
};
