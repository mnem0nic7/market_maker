//! # mm-risk
//!
//! Risk management crate for the market-maker trading platform.
//!
//! Provides:
//! - [`engine::RiskEngine`] -- central risk evaluation pipeline
//! - [`limits`] -- standard pre-trade risk checks (position, order size, PnL, rate)
//! - [`var`] -- Value-at-Risk calculations (parametric and historical)
//! - [`circuit_breaker::CircuitBreaker`] -- multi-level circuit breaker (L1--L5)
//! - [`kill_switch::KillSwitch`] -- atomic global kill switch

pub mod circuit_breaker;
pub mod engine;
pub mod kill_switch;
pub mod limits;
pub mod var;

// Re-export primary types for ergonomic imports.
pub use circuit_breaker::{
    BreakerLevel, BreakerState, CircuitBreaker, CircuitBreakerConfig, CircuitBreakerResult,
    L1Config, L2Config, L3Config, L4Config, L5Config,
};
pub use engine::RiskEngine;
pub use kill_switch::{
    ExchangeState, KillSwitch, KillSwitchActivation, KillSwitchConfig, KillSwitchTrigger,
};
pub use limits::{
    OrderSizeCheck, OrderSizeConfig, PnlLimitCheck, PnlLimitConfig, PositionLimitCheck,
    PositionLimitConfig, RateLimitCheck, RateLimitConfig,
};
pub use var::{HistoricalVar, ParametricVar, RollingVolatility};
