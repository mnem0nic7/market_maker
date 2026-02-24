use thiserror::Error;

use crate::Exchange;

#[derive(Error, Debug)]
pub enum MmError {
    // Exchange connectivity
    #[error("exchange error ({exchange}): {message}")]
    Exchange { exchange: Exchange, message: String },

    #[error("connection lost to {exchange}: {message}")]
    ConnectionLost { exchange: Exchange, message: String },

    #[error("rate limited by {exchange}, retry after {retry_after_ms}ms")]
    RateLimited { exchange: Exchange, retry_after_ms: u64 },

    #[error("authentication failed for {exchange}: {message}")]
    AuthenticationFailed { exchange: Exchange, message: String },

    // Order errors
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

    // Risk errors
    #[error("risk limit breached: {reason}")]
    RiskLimitBreached { reason: String },

    #[error("kill switch activated: {reason}")]
    KillSwitchActivated { reason: String },

    #[error("circuit breaker triggered: {reason}")]
    CircuitBreakerTriggered { reason: String },

    // Strategy errors
    #[error("strategy error ({strategy_id}): {message}")]
    Strategy { strategy_id: String, message: String },

    // Data errors
    #[error("market data unavailable for {symbol} on {exchange}")]
    MarketDataUnavailable { exchange: Exchange, symbol: String },

    #[error("stale data: last update was {age_ms}ms ago (threshold: {threshold_ms}ms)")]
    StaleData { age_ms: u64, threshold_ms: u64 },

    // Infrastructure
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
}

pub type MmResult<T> = Result<T, MmError>;
