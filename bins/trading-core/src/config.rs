//! Configuration types for the trading-core binary.
//!
//! Configuration is loaded in layers:
//! 1. `config/default.toml` — baseline defaults checked into version control.
//! 2. Environment variables prefixed with `MM_` — override individual fields
//!    (e.g. `MM_EXCHANGES__BINANCE__API_KEY` overrides
//!    `exchanges.binance.api_key`).
//!
//! The top-level [`TradingConfig`] struct mirrors the TOML structure and is
//! deserialized via the `config` crate.

use std::collections::HashMap;

use config::{Config, Environment, File};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use serde::{Deserialize, Serialize};

use mm_core::Exchange;

// ---------------------------------------------------------------------------
// Top-level config
// ---------------------------------------------------------------------------

/// Root configuration for the trading-core binary.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TradingConfig {
    /// Per-exchange connection settings (keyed by lowercase exchange name).
    #[serde(default)]
    pub exchanges: HashMap<String, ExchangeConfig>,

    /// Strategy instances to run.
    #[serde(default)]
    pub strategies: Vec<StrategyInstanceConfig>,

    /// Risk management settings.
    #[serde(default)]
    pub risk: RiskConfig,

    /// NATS event-streaming settings.
    #[serde(default)]
    pub nats: NatsStreamConfig,

    /// REST / WebSocket gateway settings.
    #[serde(default)]
    pub gateway: GatewayInstanceConfig,

    /// Main trading loop settings.
    #[serde(default)]
    pub trading_loop: TradingLoopConfig,
}

impl TradingConfig {
    /// Validate the configuration, collecting **all** errors before returning.
    ///
    /// Returns `Ok(())` when the config is valid. Non-fatal warnings (e.g. no
    /// exchange enabled) are emitted via `tracing::warn` but do not cause a
    /// failure.
    pub fn validate(&self) -> anyhow::Result<()> {
        let mut errors: Vec<String> = Vec::new();

        // 1. tick_interval_ms must be > 0 (prevents busy-loop)
        if self.trading_loop.tick_interval_ms == 0 {
            errors.push(
                "trading_loop.tick_interval_ms must be > 0 (zero causes a busy-loop)".to_string(),
            );
        }

        // 2. Enabled exchanges must have non-empty api_key and secret_key
        for (name, ec) in &self.exchanges {
            if ec.enabled {
                if ec.api_key.trim().is_empty() {
                    errors.push(format!("exchanges.{name}: enabled but api_key is empty",));
                }
                if ec.secret_key.trim().is_empty() {
                    errors.push(format!("exchanges.{name}: enabled but secret_key is empty",));
                }
            }
        }

        // 3. Risk limits must be sensible
        if self.risk.pnl_limits.max_daily_loss <= Decimal::ZERO {
            errors.push("risk.pnl_limits.max_daily_loss must be positive".to_string());
        }
        if self.risk.rate_limits.max_orders_per_second == 0 {
            errors.push("risk.rate_limits.max_orders_per_second must be > 0".to_string());
        }

        // 4. Gateway port must be in valid non-privileged range
        if self.gateway.enabled && !(1024..=65535).contains(&self.gateway.port) {
            errors.push(format!(
                "gateway.port must be in range 1024..=65535, got {}",
                self.gateway.port,
            ));
        }

        // 5. At least one exchange should be enabled (warn, not error)
        let any_enabled = self.exchanges.values().any(|ec| ec.enabled);
        if !any_enabled {
            tracing::warn!("no exchanges are enabled — the bot will not trade");
        }

        if errors.is_empty() {
            Ok(())
        } else {
            anyhow::bail!(
                "configuration validation failed:\n  - {}",
                errors.join("\n  - ")
            );
        }
    }
}

// ---------------------------------------------------------------------------
// Exchange
// ---------------------------------------------------------------------------

/// Connection and credential settings for a single exchange.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExchangeConfig {
    /// Public API key.
    #[serde(default)]
    pub api_key: String,

    /// Secret / private key.
    #[serde(default)]
    pub secret_key: String,

    /// Whether this exchange is enabled for trading.
    #[serde(default)]
    pub enabled: bool,

    /// Symbols to subscribe to on this exchange (e.g. ["BTC/USDT", "ETH/USDT"]).
    #[serde(default)]
    pub symbols: Vec<String>,
}

// ---------------------------------------------------------------------------
// Strategy
// ---------------------------------------------------------------------------

/// Configuration for a single strategy instance.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StrategyInstanceConfig {
    /// Unique identifier for this strategy instance.
    pub strategy_id: String,

    /// Strategy type: "spread", "avellaneda", or "grid".
    pub strategy_type: String,

    /// Target exchange name (must match a key in `exchanges`).
    pub exchange: String,

    /// Trading symbol (e.g. "BTC/USDT").
    pub symbol: String,

    /// Whether this strategy instance is enabled.
    #[serde(default = "default_true")]
    pub enabled: bool,

    /// Strategy-specific parameters (passed as opaque JSON to the strategy
    /// constructor).
    #[serde(default = "default_empty_object")]
    pub params: serde_json::Value,
}

fn default_true() -> bool {
    true
}

fn default_empty_object() -> serde_json::Value {
    serde_json::Value::Object(serde_json::Map::new())
}

// ---------------------------------------------------------------------------
// Risk
// ---------------------------------------------------------------------------

/// Risk management configuration.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RiskConfig {
    /// Position limit settings.
    #[serde(default)]
    pub position_limits: PositionLimitSettings,

    /// Single-order size limits.
    #[serde(default)]
    pub order_limits: OrderLimitSettings,

    /// PnL / drawdown limits.
    #[serde(default)]
    pub pnl_limits: PnlLimitSettings,

    /// Rate limits per exchange.
    #[serde(default)]
    pub rate_limits: RateLimitSettings,

    /// Kill switch settings.
    #[serde(default)]
    pub kill_switch: KillSwitchSettings,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PositionLimitSettings {
    /// Maximum absolute position size for any single symbol.
    #[serde(with = "rust_decimal::serde::str")]
    pub max_position_per_symbol: Decimal,

    /// Maximum total gross exposure across all positions.
    #[serde(with = "rust_decimal::serde::str")]
    pub max_gross_exposure: Decimal,
}

impl Default for PositionLimitSettings {
    fn default() -> Self {
        Self {
            max_position_per_symbol: dec!(10),
            max_gross_exposure: dec!(500_000),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderLimitSettings {
    /// Maximum quantity for a single order.
    #[serde(with = "rust_decimal::serde::str")]
    pub max_order_quantity: Decimal,

    /// Maximum notional value for a single order.
    #[serde(with = "rust_decimal::serde::str")]
    pub max_order_notional: Decimal,
}

impl Default for OrderLimitSettings {
    fn default() -> Self {
        Self {
            max_order_quantity: dec!(1),
            max_order_notional: dec!(100_000),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PnlLimitSettings {
    /// Maximum daily loss (positive number; triggers kill switch when
    /// daily_pnl <= -max_daily_loss).
    #[serde(with = "rust_decimal::serde::str")]
    pub max_daily_loss: Decimal,

    /// Maximum drawdown as a decimal fraction (0.05 = 5%).
    #[serde(with = "rust_decimal::serde::str")]
    pub max_drawdown_pct: Decimal,
}

impl Default for PnlLimitSettings {
    fn default() -> Self {
        Self {
            max_daily_loss: dec!(10_000),
            max_drawdown_pct: dec!(0.05),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RateLimitSettings {
    /// Maximum order submissions per second per exchange.
    #[serde(default = "default_rate_per_second")]
    pub max_orders_per_second: u32,

    /// Maximum order submissions per minute per exchange.
    #[serde(default = "default_rate_per_minute")]
    pub max_orders_per_minute: u32,
}

fn default_rate_per_second() -> u32 {
    10
}

fn default_rate_per_minute() -> u32 {
    200
}

impl Default for RateLimitSettings {
    fn default() -> Self {
        Self {
            max_orders_per_second: default_rate_per_second(),
            max_orders_per_minute: default_rate_per_minute(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KillSwitchSettings {
    /// Maximum daily loss before the kill switch fires (positive number).
    #[serde(with = "rust_decimal::serde::str")]
    pub max_daily_loss: Decimal,

    /// Maximum position size per symbol before the kill switch fires.
    #[serde(with = "rust_decimal::serde::str")]
    pub max_position_per_symbol: Decimal,

    /// Maximum seconds an exchange can be disconnected before kill switch.
    #[serde(default = "default_disconnect_timeout")]
    pub exchange_disconnect_timeout_secs: u64,

    /// Maximum seconds without an internal heartbeat before kill switch.
    #[serde(default = "default_heartbeat_timeout")]
    pub heartbeat_timeout_secs: u64,
}

fn default_disconnect_timeout() -> u64 {
    30
}

fn default_heartbeat_timeout() -> u64 {
    10
}

impl Default for KillSwitchSettings {
    fn default() -> Self {
        Self {
            max_daily_loss: dec!(10_000),
            max_position_per_symbol: dec!(100),
            exchange_disconnect_timeout_secs: default_disconnect_timeout(),
            heartbeat_timeout_secs: default_heartbeat_timeout(),
        }
    }
}

// ---------------------------------------------------------------------------
// NATS
// ---------------------------------------------------------------------------

/// NATS event streaming configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NatsStreamConfig {
    /// Whether NATS publishing is enabled.
    #[serde(default)]
    pub enabled: bool,

    /// NATS server URL (e.g. "nats://127.0.0.1:4222").
    #[serde(default = "default_nats_url")]
    pub url: String,

    /// JetStream stream name.
    #[serde(default = "default_nats_stream")]
    pub stream_name: String,

    /// Maximum message age in seconds (0 = unlimited).
    #[serde(default)]
    pub max_age_secs: u64,

    /// Maximum total bytes in the stream (0 = unlimited).
    #[serde(default)]
    pub max_bytes: i64,
}

fn default_nats_url() -> String {
    "nats://127.0.0.1:4222".to_string()
}

fn default_nats_stream() -> String {
    "EVENTS".to_string()
}

impl Default for NatsStreamConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            url: default_nats_url(),
            stream_name: default_nats_stream(),
            max_age_secs: 0,
            max_bytes: 0,
        }
    }
}

// ---------------------------------------------------------------------------
// Gateway
// ---------------------------------------------------------------------------

/// REST / WebSocket gateway configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GatewayInstanceConfig {
    /// Whether the gateway is enabled.
    #[serde(default = "default_true")]
    pub enabled: bool,

    /// Bind host address.
    #[serde(default = "default_gateway_host")]
    pub host: String,

    /// Bind port.
    #[serde(default = "default_gateway_port")]
    pub port: u16,

    /// Allowed CORS origins. If empty, falls back to wildcard.
    #[serde(default = "default_cors_origins")]
    pub cors_origins: Vec<String>,

    /// Optional API bearer token. If set, all REST endpoints require
    /// `Authorization: Bearer <token>` header. If empty/None, auth is disabled.
    #[serde(default)]
    pub api_token: Option<String>,
}

fn default_gateway_host() -> String {
    "127.0.0.1".to_string()
}

fn default_gateway_port() -> u16 {
    8080
}

fn default_cors_origins() -> Vec<String> {
    vec![
        "http://127.0.0.1:3000".to_string(),
        "http://localhost:3000".to_string(),
    ]
}

impl Default for GatewayInstanceConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            host: default_gateway_host(),
            port: default_gateway_port(),
            cors_origins: default_cors_origins(),
            api_token: None,
        }
    }
}

// ---------------------------------------------------------------------------
// Trading loop
// ---------------------------------------------------------------------------

/// Settings for the main event loop.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradingLoopConfig {
    /// Interval in milliseconds between strategy tick evaluations.
    #[serde(default = "default_tick_interval")]
    pub tick_interval_ms: u64,
}

fn default_tick_interval() -> u64 {
    100
}

impl Default for TradingLoopConfig {
    fn default() -> Self {
        Self {
            tick_interval_ms: default_tick_interval(),
        }
    }
}

// ---------------------------------------------------------------------------
// Helper: parse exchange name to Exchange enum
// ---------------------------------------------------------------------------

/// Parse a lowercase exchange key (from config) into the [`Exchange`] enum.
pub fn parse_exchange(name: &str) -> Option<Exchange> {
    match name.to_lowercase().as_str() {
        "binance" => Some(Exchange::Binance),
        "coinbase" => Some(Exchange::Coinbase),
        "kraken" => Some(Exchange::Kraken),
        "alpaca" => Some(Exchange::Alpaca),
        "interactive_brokers" | "ib" => Some(Exchange::InteractiveBrokers),
        "uniswap" => Some(Exchange::Uniswap),
        "raydium" => Some(Exchange::Raydium),
        "simulated" => Some(Exchange::Simulated),
        _ => None,
    }
}

/// Parse a symbol string like "BTC/USDT" into a [`mm_core::Symbol`].
pub fn parse_symbol(s: &str) -> Option<mm_core::Symbol> {
    let parts: Vec<&str> = s.split('/').collect();
    if parts.len() == 2 {
        Some(mm_core::Symbol::new(parts[0], parts[1]))
    } else {
        None
    }
}

// ---------------------------------------------------------------------------
// Loader
// ---------------------------------------------------------------------------

/// Load the [`TradingConfig`] from `config/default.toml` and environment
/// variable overrides prefixed with `MM_`.
pub fn load_config() -> anyhow::Result<TradingConfig> {
    let settings = Config::builder()
        .add_source(File::with_name("config/default").required(false))
        .add_source(
            Environment::with_prefix("MM")
                .separator("__")
                .try_parsing(true),
        )
        .build()?;

    let mut config: TradingConfig = settings.try_deserialize()?;

    // The `config` crate's Environment source doesn't reliably override
    // fields inside HashMap entries. Manually apply env var overrides for
    // exchange credentials (the most important case).
    apply_exchange_env_overrides(&mut config);

    Ok(config)
}

/// Scan for `MM_EXCHANGES__<NAME>__API_KEY` / `SECRET_KEY` / `ENABLED` env
/// vars and apply them to the corresponding exchange config entries.
fn apply_exchange_env_overrides(config: &mut TradingConfig) {
    let exchange_names: Vec<String> = config.exchanges.keys().cloned().collect();

    for name in exchange_names {
        let upper = name.to_uppercase();

        if let Ok(val) = std::env::var(format!("MM_EXCHANGES__{upper}__API_KEY")) {
            if let Some(ec) = config.exchanges.get_mut(&name) {
                ec.api_key = val;
            }
        }
        if let Ok(val) = std::env::var(format!("MM_EXCHANGES__{upper}__SECRET_KEY")) {
            if let Some(ec) = config.exchanges.get_mut(&name) {
                ec.secret_key = val;
            }
        }
        if let Ok(val) = std::env::var(format!("MM_EXCHANGES__{upper}__ENABLED")) {
            if let Some(ec) = config.exchanges.get_mut(&name) {
                ec.enabled = val.eq_ignore_ascii_case("true") || val == "1";
            }
        }
    }
}
