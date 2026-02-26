//! Error types for the market maker platform.

use thiserror::Error;

use crate::Exchange;

/// Unified error type covering all failure modes in the market maker platform.
///
/// Variants are grouped by domain: exchange connectivity, order management,
/// risk limits, strategy logic, market data, and infrastructure.
#[derive(Error, Debug)]
pub enum MmError {
    // -- Exchange connectivity --
    #[error("exchange error ({exchange}): {message}")]
    Exchange { exchange: Exchange, message: String },

    #[error("connection lost to {exchange}: {message}")]
    ConnectionLost { exchange: Exchange, message: String },

    #[error("rate limited by {exchange}, retry after {retry_after_ms}ms")]
    RateLimited {
        exchange: Exchange,
        retry_after_ms: u64,
    },

    #[error("authentication failed for {exchange}: {message}")]
    AuthenticationFailed { exchange: Exchange, message: String },

    // -- Order errors --
    #[error("order rejected: {reason}")]
    OrderRejected { reason: String },

    #[error("order not found: {order_id}")]
    OrderNotFound { order_id: String },

    #[error("insufficient balance: need {required}, have {available} {currency}")]
    InsufficientBalance {
        required: String,
        available: String,
        currency: String,
    },

    // -- Risk errors --
    #[error("risk limit breached: {reason}")]
    RiskLimitBreached { reason: String },

    #[error("kill switch activated: {reason}")]
    KillSwitchActivated { reason: String },

    #[error("circuit breaker triggered: {reason}")]
    CircuitBreakerTriggered { reason: String },

    // -- Strategy errors --
    #[error("strategy error ({strategy_id}): {message}")]
    Strategy {
        strategy_id: String,
        message: String,
    },

    // -- Data errors --
    #[error("market data unavailable for {symbol} on {exchange}")]
    MarketDataUnavailable { exchange: Exchange, symbol: String },

    #[error("stale data: last update was {age_ms}ms ago (threshold: {threshold_ms}ms)")]
    StaleData { age_ms: u64, threshold_ms: u64 },

    // -- DEX safety --
    #[error("DEX connector not production-ready ({exchange}): {message}")]
    DexNotReady { exchange: Exchange, message: String },

    // -- Infrastructure --
    #[error("database error: {0}")]
    Database(String),

    #[error("messaging error: {0}")]
    Messaging(String),

    #[error("config error: {0}")]
    Config(String),

    #[error("serialization error: {0}")]
    Serialization(String),

    #[error("{0}")]
    Internal(String),

    // -- Contextual wrapper --
    /// Wraps an existing `MmError` with additional context, preserving the
    /// full error source chain via `std::error::Error::source()`.
    #[error("{context}")]
    WithContext {
        context: String,
        #[source]
        source: Box<MmError>,
    },
}

impl MmError {
    /// Wrap this error with additional context. The original error is
    /// preserved as the `source` of the returned `WithContext` variant,
    /// enabling callers to walk the full error chain via
    /// `std::error::Error::source()`.
    pub fn with_context(self, context: impl Into<String>) -> Self {
        MmError::WithContext {
            context: context.into(),
            source: Box::new(self),
        }
    }
}

/// Convenience type alias for results throughout the market maker platform.
pub type MmResult<T> = Result<T, MmError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_with_context_preserves_source() {
        let inner = MmError::OrderNotFound {
            order_id: "abc-123".to_string(),
        };
        let outer = inner.with_context("failed to cancel order");

        // The outer error should be a WithContext variant.
        match &outer {
            MmError::WithContext { context, source } => {
                assert_eq!(context, "failed to cancel order");
                // The inner source should be the original OrderNotFound.
                match source.as_ref() {
                    MmError::OrderNotFound { order_id } => {
                        assert_eq!(order_id, "abc-123");
                    }
                    other => panic!("expected OrderNotFound, got: {other:?}"),
                }
            }
            other => panic!("expected WithContext, got: {other:?}"),
        }
    }

    #[test]
    fn test_error_display_shows_context() {
        let inner = MmError::Exchange {
            exchange: Exchange::Binance,
            message: "timeout".to_string(),
        };
        let outer = inner.with_context("placing limit order");

        // Display should show the context string, not the inner error.
        assert_eq!(outer.to_string(), "placing limit order");
    }

    #[test]
    fn test_error_source_chain() {
        use std::error::Error;

        let inner = MmError::RiskLimitBreached {
            reason: "position too large".to_string(),
        };
        let mid = inner.with_context("evaluating new order");
        let outer = mid.with_context("strategy tick failed");

        // Walk the chain: outer -> mid -> inner
        assert_eq!(outer.to_string(), "strategy tick failed");

        let mid_err = outer.source().expect("outer should have a source");
        assert_eq!(mid_err.to_string(), "evaluating new order");

        let inner_err = mid_err.source().expect("mid should have a source");
        assert_eq!(
            inner_err.to_string(),
            "risk limit breached: position too large"
        );

        // The innermost error should have no further source.
        assert!(inner_err.source().is_none());
    }

    #[test]
    fn test_error_with_context_multiple_levels() {
        let base = MmError::Database("connection refused".to_string());
        let wrapped = base
            .with_context("persisting fill")
            .with_context("processing order update");

        assert_eq!(wrapped.to_string(), "processing order update");

        // Verify the full chain is 3 levels deep.
        use std::error::Error;
        let level1 = wrapped.source().unwrap();
        assert_eq!(level1.to_string(), "persisting fill");

        let level2 = level1.source().unwrap();
        assert_eq!(level2.to_string(), "database error: connection refused");

        assert!(level2.source().is_none());
    }
}
