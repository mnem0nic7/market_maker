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
        if self.risk.pnl_limits.max_drawdown_pct <= Decimal::ZERO {
            errors.push("risk.pnl_limits.max_drawdown_pct must be positive".to_string());
        }
        if self.risk.rate_limits.max_orders_per_second == 0 {
            errors.push("risk.rate_limits.max_orders_per_second must be > 0".to_string());
        }
        if self.risk.position_limits.max_position_per_symbol <= Decimal::ZERO {
            errors
                .push("risk.position_limits.max_position_per_symbol must be positive".to_string());
        }
        if self.risk.position_limits.max_gross_exposure <= Decimal::ZERO {
            errors.push("risk.position_limits.max_gross_exposure must be positive".to_string());
        }

        // 4. Gateway port must be in valid non-privileged range
        if self.gateway.enabled && !(1024..=65535).contains(&self.gateway.port) {
            errors.push(format!(
                "gateway.port must be in range 1024..=65535, got {}",
                self.gateway.port,
            ));
        }

        // 5. order_book_stale_ms must be > 0 (zero means everything is stale)
        if self.trading_loop.order_book_stale_ms == 0 {
            errors.push(
                "trading_loop.order_book_stale_ms must be > 0 (zero means all books are stale)"
                    .to_string(),
            );
        }

        // 6. Risk limit consistency: kill_switch.max_daily_loss should be >=
        //    pnl_limits.max_daily_loss (the pnl check fires first)
        if self.risk.kill_switch.max_daily_loss < self.risk.pnl_limits.max_daily_loss {
            errors.push(format!(
                "risk.kill_switch.max_daily_loss ({}) is less than risk.pnl_limits.max_daily_loss ({}); \
                 the kill switch should have a higher or equal threshold",
                self.risk.kill_switch.max_daily_loss, self.risk.pnl_limits.max_daily_loss,
            ));
        }

        // 7. At least one exchange should be enabled (warn, not error)
        let any_enabled = self.exchanges.values().any(|ec| ec.enabled);
        if !any_enabled {
            tracing::warn!("no exchanges are enabled — the bot will not trade");
        }

        // 8. Disabled exchanges with empty credentials get an informational warn
        for (name, ec) in &self.exchanges {
            if !ec.enabled && ec.api_key.trim().is_empty() {
                tracing::warn!(
                    "exchanges.{}: api_key is empty (exchange is disabled, so this is non-fatal)",
                    name,
                );
            }
        }

        // 9. Strategy types must be known
        const KNOWN_STRATEGY_TYPES: &[&str] = &[
            "spread",
            "avellaneda",
            "grid",
            "twap",
            "vwap",
            "momentum",
            "mean_reversion",
            "stat_arb",
        ];
        for strat in &self.strategies {
            if !strat.enabled {
                continue;
            }
            if !KNOWN_STRATEGY_TYPES.contains(&strat.strategy_type.as_str()) {
                errors.push(format!(
                    "strategies[{}]: unknown strategy_type '{}' (known: {})",
                    strat.strategy_id,
                    strat.strategy_type,
                    KNOWN_STRATEGY_TYPES.join(", "),
                ));
            }
        }

        // 10. Enabled strategies must reference an enabled exchange
        for strat in &self.strategies {
            if !strat.enabled {
                continue;
            }
            let exchange_key = strat.exchange.to_lowercase();
            match self.exchanges.get(&exchange_key) {
                None => {
                    errors.push(format!(
                        "strategies[{}]: exchange '{}' not found in exchanges config",
                        strat.strategy_id, strat.exchange,
                    ));
                }
                Some(ec) if !ec.enabled => {
                    // Warn rather than error — user might intend to enable the exchange later
                    tracing::warn!(
                        "strategies[{}]: exchange '{}' is not enabled",
                        strat.strategy_id,
                        strat.exchange,
                    );
                }
                _ => {}
            }
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

    /// Maximum age in milliseconds for an order book to be considered fresh.
    /// Books older than this threshold are skipped to prevent trading on stale
    /// data. Defaults to 5000 (5 seconds).
    #[serde(default = "default_order_book_stale_ms")]
    pub order_book_stale_ms: u64,

    /// Interval in seconds between position reconciliation checks against
    /// exchange-reported balances. Defaults to 300 (5 minutes). Set to 0 to
    /// disable reconciliation.
    #[serde(default = "default_reconciliation_interval_secs")]
    pub reconciliation_interval_secs: u64,

    /// Drift percentage threshold that triggers a warning log during
    /// reconciliation. Defaults to 1.0 (1%). Drift above 5% triggers an
    /// error log.
    #[serde(default = "default_reconciliation_drift_threshold_pct")]
    pub reconciliation_drift_threshold_pct: f64,
}

fn default_tick_interval() -> u64 {
    100
}

fn default_order_book_stale_ms() -> u64 {
    5000
}

fn default_reconciliation_interval_secs() -> u64 {
    300
}

fn default_reconciliation_drift_threshold_pct() -> f64 {
    1.0
}

impl Default for TradingLoopConfig {
    fn default() -> Self {
        Self {
            tick_interval_ms: default_tick_interval(),
            order_book_stale_ms: default_order_book_stale_ms(),
            reconciliation_interval_secs: default_reconciliation_interval_secs(),
            reconciliation_drift_threshold_pct: default_reconciliation_drift_threshold_pct(),
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

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    /// Helper: build a minimal valid TradingConfig.
    ///
    /// All defaults are valid, but we explicitly set one disabled exchange
    /// entry so the exchange map is non-empty (matching real-world usage).
    fn make_valid_config() -> TradingConfig {
        let mut cfg = TradingConfig::default();
        cfg.exchanges.insert(
            "simulated".to_string(),
            ExchangeConfig {
                api_key: String::new(),
                secret_key: String::new(),
                enabled: false,
                symbols: vec!["BTC/USDT".to_string()],
            },
        );
        cfg
    }

    /// Helper: build a config with one *enabled* exchange that has credentials.
    fn make_config_with_enabled_exchange() -> TradingConfig {
        let mut cfg = TradingConfig::default();
        cfg.exchanges.insert(
            "binance".to_string(),
            ExchangeConfig {
                api_key: "test-api-key".to_string(),
                secret_key: "test-secret-key".to_string(),
                enabled: true,
                symbols: vec!["BTC/USDT".to_string()],
            },
        );
        cfg
    }

    // -----------------------------------------------------------------------
    // 1. Default config validates
    // -----------------------------------------------------------------------
    #[test]
    fn test_default_config_validates() {
        let cfg = TradingConfig::default();
        assert!(
            cfg.validate().is_ok(),
            "default config should pass validation: {:?}",
            cfg.validate().err(),
        );
    }

    // -----------------------------------------------------------------------
    // 2. Zero tick_interval_ms fails
    // -----------------------------------------------------------------------
    #[test]
    fn test_zero_tick_interval_fails() {
        let mut cfg = make_valid_config();
        cfg.trading_loop.tick_interval_ms = 0;
        let err = cfg.validate().unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("tick_interval_ms must be > 0"),
            "expected tick_interval error, got: {msg}",
        );
    }

    // -----------------------------------------------------------------------
    // 3. Negative risk limits fail (position limits)
    // -----------------------------------------------------------------------
    #[test]
    fn test_negative_position_limit_fails() {
        let mut cfg = make_valid_config();
        cfg.risk.position_limits.max_position_per_symbol = dec!(-5);
        let err = cfg.validate().unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("max_position_per_symbol must be positive"),
            "expected position limit error, got: {msg}",
        );
    }

    #[test]
    fn test_zero_max_gross_exposure_fails() {
        let mut cfg = make_valid_config();
        cfg.risk.position_limits.max_gross_exposure = Decimal::ZERO;
        let err = cfg.validate().unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("max_gross_exposure must be positive"),
            "expected gross exposure error, got: {msg}",
        );
    }

    #[test]
    fn test_negative_max_daily_loss_fails() {
        let mut cfg = make_valid_config();
        cfg.risk.pnl_limits.max_daily_loss = dec!(-100);
        let err = cfg.validate().unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("max_daily_loss must be positive"),
            "expected pnl limit error, got: {msg}",
        );
    }

    #[test]
    fn test_negative_max_drawdown_pct_fails() {
        let mut cfg = make_valid_config();
        cfg.risk.pnl_limits.max_drawdown_pct = dec!(-0.05);
        let err = cfg.validate().unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("max_drawdown_pct must be positive"),
            "expected drawdown pct error, got: {msg}",
        );
    }

    // -----------------------------------------------------------------------
    // 4. Empty exchange list (warn, not fail)
    // -----------------------------------------------------------------------
    #[test]
    fn test_empty_exchange_list_still_validates() {
        // No exchanges at all — should still pass validation (warn only).
        let cfg = TradingConfig::default();
        assert!(cfg.exchanges.is_empty());
        assert!(
            cfg.validate().is_ok(),
            "empty exchange list should warn but not fail",
        );
    }

    // -----------------------------------------------------------------------
    // 5. Invalid gateway port fails
    // -----------------------------------------------------------------------
    #[test]
    fn test_gateway_port_zero_fails() {
        let mut cfg = make_valid_config();
        cfg.gateway.enabled = true;
        cfg.gateway.port = 0;
        let err = cfg.validate().unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("gateway.port must be in range"),
            "expected port range error, got: {msg}",
        );
    }

    #[test]
    fn test_gateway_privileged_port_fails() {
        let mut cfg = make_valid_config();
        cfg.gateway.enabled = true;
        cfg.gateway.port = 80; // privileged port
        let err = cfg.validate().unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("gateway.port must be in range"),
            "expected port range error, got: {msg}",
        );
    }

    #[test]
    fn test_gateway_port_1024_is_ok() {
        let mut cfg = make_valid_config();
        cfg.gateway.enabled = true;
        cfg.gateway.port = 1024;
        assert!(
            cfg.validate().is_ok(),
            "port 1024 is the minimum valid non-privileged port",
        );
    }

    #[test]
    fn test_gateway_disabled_skips_port_validation() {
        let mut cfg = make_valid_config();
        cfg.gateway.enabled = false;
        cfg.gateway.port = 0; // invalid, but gateway is disabled
        assert!(
            cfg.validate().is_ok(),
            "disabled gateway should not validate port",
        );
    }

    // -----------------------------------------------------------------------
    // 6. Zero order_book_stale_ms fails
    // -----------------------------------------------------------------------
    #[test]
    fn test_zero_order_book_stale_ms_fails() {
        let mut cfg = make_valid_config();
        cfg.trading_loop.order_book_stale_ms = 0;
        let err = cfg.validate().unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("order_book_stale_ms must be > 0"),
            "expected stale ms error, got: {msg}",
        );
    }

    // -----------------------------------------------------------------------
    // 7. Zero reconciliation_interval_secs is OK (means disabled)
    // -----------------------------------------------------------------------
    #[test]
    fn test_zero_reconciliation_interval_ok() {
        let mut cfg = make_valid_config();
        cfg.trading_loop.reconciliation_interval_secs = 0; // disabled
        assert!(
            cfg.validate().is_ok(),
            "zero reconciliation interval means disabled, should pass",
        );
    }

    // -----------------------------------------------------------------------
    // 8. Valid minimal config
    // -----------------------------------------------------------------------
    #[test]
    fn test_valid_minimal_config() {
        // A truly minimal config: all defaults, nothing overridden.
        let cfg = TradingConfig::default();
        assert!(cfg.validate().is_ok());
    }

    // -----------------------------------------------------------------------
    // 9. TOML serialization round-trip
    // -----------------------------------------------------------------------
    #[test]
    fn test_config_serialization_round_trip() {
        let original = make_config_with_enabled_exchange();

        // Serialize to TOML
        let toml_str = toml::to_string(&original).expect("TradingConfig should serialize to TOML");

        // Deserialize back
        let restored: TradingConfig =
            toml::from_str(&toml_str).expect("TOML should deserialize back to TradingConfig");

        // Verify key fields survived the round-trip
        assert_eq!(
            original.trading_loop.tick_interval_ms,
            restored.trading_loop.tick_interval_ms,
        );
        assert_eq!(
            original.risk.pnl_limits.max_daily_loss,
            restored.risk.pnl_limits.max_daily_loss,
        );
        assert_eq!(original.gateway.port, restored.gateway.port,);
        assert_eq!(
            original.trading_loop.order_book_stale_ms,
            restored.trading_loop.order_book_stale_ms,
        );
        // Check exchange entry survived
        assert!(restored.exchanges.contains_key("binance"));
        let binance = &restored.exchanges["binance"];
        assert_eq!(binance.api_key, "test-api-key");
        assert!(binance.enabled);
    }

    // -----------------------------------------------------------------------
    // 10. Environment override works (manual apply)
    // -----------------------------------------------------------------------
    #[test]
    fn test_env_override_works() {
        // Set env vars before calling the override helper
        let unique_key = format!("TEST_KEY_{}", std::process::id());
        let unique_secret = format!("TEST_SECRET_{}", std::process::id());

        let mut cfg = TradingConfig::default();
        cfg.exchanges.insert(
            "binance".to_string(),
            ExchangeConfig {
                api_key: "placeholder".to_string(),
                secret_key: "placeholder".to_string(),
                enabled: false,
                symbols: vec![],
            },
        );

        // Set the env vars that apply_exchange_env_overrides looks for
        std::env::set_var("MM_EXCHANGES__BINANCE__API_KEY", &unique_key);
        std::env::set_var("MM_EXCHANGES__BINANCE__SECRET_KEY", &unique_secret);
        std::env::set_var("MM_EXCHANGES__BINANCE__ENABLED", "true");

        apply_exchange_env_overrides(&mut cfg);

        let binance = &cfg.exchanges["binance"];
        assert_eq!(binance.api_key, unique_key);
        assert_eq!(binance.secret_key, unique_secret);
        assert!(binance.enabled);

        // Clean up
        std::env::remove_var("MM_EXCHANGES__BINANCE__API_KEY");
        std::env::remove_var("MM_EXCHANGES__BINANCE__SECRET_KEY");
        std::env::remove_var("MM_EXCHANGES__BINANCE__ENABLED");
    }

    // -----------------------------------------------------------------------
    // 11. Missing API key on disabled exchange warns but does not fail
    // -----------------------------------------------------------------------
    #[test]
    fn test_missing_api_key_on_disabled_exchange_does_not_fail() {
        let mut cfg = TradingConfig::default();
        cfg.exchanges.insert(
            "kraken".to_string(),
            ExchangeConfig {
                api_key: String::new(), // empty
                secret_key: String::new(),
                enabled: false, // disabled — so empty key should NOT cause error
                symbols: vec![],
            },
        );
        assert!(
            cfg.validate().is_ok(),
            "disabled exchange with empty api_key should pass (warn only)",
        );
    }

    #[test]
    fn test_enabled_exchange_with_empty_api_key_fails() {
        let mut cfg = make_valid_config();
        cfg.exchanges.insert(
            "binance".to_string(),
            ExchangeConfig {
                api_key: "  ".to_string(), // whitespace-only
                secret_key: "valid-secret".to_string(),
                enabled: true,
                symbols: vec!["BTC/USDT".to_string()],
            },
        );
        let err = cfg.validate().unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("api_key is empty"),
            "expected api_key empty error, got: {msg}",
        );
    }

    // -----------------------------------------------------------------------
    // 12. Risk limit consistency (kill switch >= pnl limit)
    // -----------------------------------------------------------------------
    #[test]
    fn test_risk_limit_consistency_kill_switch_below_pnl_fails() {
        let mut cfg = make_valid_config();
        cfg.risk.pnl_limits.max_daily_loss = dec!(20_000);
        cfg.risk.kill_switch.max_daily_loss = dec!(10_000); // less than pnl limit
        let err = cfg.validate().unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("kill_switch.max_daily_loss") && msg.contains("less than"),
            "expected kill switch consistency error, got: {msg}",
        );
    }

    #[test]
    fn test_risk_limit_consistency_equal_values_ok() {
        let mut cfg = make_valid_config();
        cfg.risk.pnl_limits.max_daily_loss = dec!(10_000);
        cfg.risk.kill_switch.max_daily_loss = dec!(10_000);
        assert!(
            cfg.validate().is_ok(),
            "equal kill switch and pnl daily loss should be valid",
        );
    }

    // -----------------------------------------------------------------------
    // 13. Zero max_orders_per_second fails
    // -----------------------------------------------------------------------
    #[test]
    fn test_zero_max_orders_per_second_fails() {
        let mut cfg = make_valid_config();
        cfg.risk.rate_limits.max_orders_per_second = 0;
        let err = cfg.validate().unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("max_orders_per_second must be > 0"),
            "expected rate limit error, got: {msg}",
        );
    }

    // -----------------------------------------------------------------------
    // 14. Multiple errors are collected and reported together
    // -----------------------------------------------------------------------
    #[test]
    fn test_multiple_errors_collected() {
        let mut cfg = make_valid_config();
        cfg.trading_loop.tick_interval_ms = 0;
        cfg.trading_loop.order_book_stale_ms = 0;
        cfg.risk.pnl_limits.max_daily_loss = Decimal::ZERO;
        cfg.risk.rate_limits.max_orders_per_second = 0;

        let err = cfg.validate().unwrap_err();
        let msg = err.to_string();

        // All four errors should be present in one message
        assert!(
            msg.contains("tick_interval_ms"),
            "missing tick error in: {msg}"
        );
        assert!(
            msg.contains("order_book_stale_ms"),
            "missing stale error in: {msg}"
        );
        assert!(
            msg.contains("max_daily_loss"),
            "missing pnl error in: {msg}"
        );
        assert!(
            msg.contains("max_orders_per_second"),
            "missing rate error in: {msg}"
        );
    }

    // -----------------------------------------------------------------------
    // 15. parse_exchange helper
    // -----------------------------------------------------------------------
    #[test]
    fn test_parse_exchange_known() {
        assert_eq!(parse_exchange("binance"), Some(Exchange::Binance));
        assert_eq!(parse_exchange("Coinbase"), Some(Exchange::Coinbase));
        assert_eq!(parse_exchange("KRAKEN"), Some(Exchange::Kraken));
        assert_eq!(parse_exchange("alpaca"), Some(Exchange::Alpaca));
        assert_eq!(
            parse_exchange("interactive_brokers"),
            Some(Exchange::InteractiveBrokers)
        );
        assert_eq!(parse_exchange("ib"), Some(Exchange::InteractiveBrokers));
        assert_eq!(parse_exchange("simulated"), Some(Exchange::Simulated));
    }

    #[test]
    fn test_parse_exchange_unknown_returns_none() {
        assert_eq!(parse_exchange("unknown_exchange"), None);
        assert_eq!(parse_exchange(""), None);
    }

    // -----------------------------------------------------------------------
    // 16. parse_symbol helper
    // -----------------------------------------------------------------------
    #[test]
    fn test_parse_symbol_valid() {
        let sym = parse_symbol("BTC/USDT").expect("valid symbol");
        assert_eq!(sym.base, "BTC");
        assert_eq!(sym.quote, "USDT");
    }

    #[test]
    fn test_parse_symbol_invalid() {
        assert!(parse_symbol("BTCUSDT").is_none(), "no slash should fail");
        assert!(parse_symbol("BTC/USDT/extra").is_none(), "too many slashes");
        assert!(parse_symbol("").is_none(), "empty string");
    }

    // -----------------------------------------------------------------------
    // 17. Unknown strategy type fails validation
    // -----------------------------------------------------------------------
    #[test]
    fn test_unknown_strategy_type_fails() {
        let mut cfg = make_config_with_enabled_exchange();
        cfg.strategies.push(StrategyInstanceConfig {
            strategy_id: "test-strat".to_string(),
            strategy_type: "nonexistent".to_string(),
            exchange: "binance".to_string(),
            symbol: "BTC/USDT".to_string(),
            enabled: true,
            params: serde_json::json!({}),
        });
        let err = cfg.validate().unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("unknown strategy_type"),
            "expected unknown strategy_type error, got: {msg}",
        );
    }

    // -----------------------------------------------------------------------
    // 18. All known strategy types pass validation
    // -----------------------------------------------------------------------
    #[test]
    fn test_known_strategy_types_pass() {
        let known_types = [
            "spread",
            "avellaneda",
            "grid",
            "twap",
            "vwap",
            "momentum",
            "mean_reversion",
            "stat_arb",
        ];
        let mut cfg = make_config_with_enabled_exchange();
        for (i, strategy_type) in known_types.iter().enumerate() {
            cfg.strategies.push(StrategyInstanceConfig {
                strategy_id: format!("strat-{i}"),
                strategy_type: strategy_type.to_string(),
                exchange: "binance".to_string(),
                symbol: "BTC/USDT".to_string(),
                enabled: true,
                params: serde_json::json!({}),
            });
        }
        assert!(
            cfg.validate().is_ok(),
            "all known strategy types should pass validation: {:?}",
            cfg.validate().err(),
        );
    }

    // -----------------------------------------------------------------------
    // 19. Strategy referencing missing exchange fails
    // -----------------------------------------------------------------------
    #[test]
    fn test_strategy_references_missing_exchange_fails() {
        let mut cfg = make_config_with_enabled_exchange();
        cfg.strategies.push(StrategyInstanceConfig {
            strategy_id: "test-strat".to_string(),
            strategy_type: "spread".to_string(),
            exchange: "doesnotexist".to_string(),
            symbol: "BTC/USDT".to_string(),
            enabled: true,
            params: serde_json::json!({}),
        });
        let err = cfg.validate().unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("not found in exchanges config"),
            "expected missing exchange error, got: {msg}",
        );
    }

    // -----------------------------------------------------------------------
    // 20. Strategy referencing disabled exchange warns but passes
    // -----------------------------------------------------------------------
    #[test]
    fn test_strategy_references_disabled_exchange_warns_but_passes() {
        let mut cfg = TradingConfig::default();
        cfg.exchanges.insert(
            "binance".to_string(),
            ExchangeConfig {
                api_key: String::new(),
                secret_key: String::new(),
                enabled: false, // disabled
                symbols: vec!["BTC/USDT".to_string()],
            },
        );
        cfg.strategies.push(StrategyInstanceConfig {
            strategy_id: "test-strat".to_string(),
            strategy_type: "spread".to_string(),
            exchange: "binance".to_string(),
            symbol: "BTC/USDT".to_string(),
            enabled: true,
            params: serde_json::json!({}),
        });
        assert!(
            cfg.validate().is_ok(),
            "strategy referencing disabled exchange should warn but pass: {:?}",
            cfg.validate().err(),
        );
    }

    // -----------------------------------------------------------------------
    // 21. Disabled strategy skips all validation
    // -----------------------------------------------------------------------
    #[test]
    fn test_disabled_strategy_skips_validation() {
        let mut cfg = make_valid_config();
        cfg.strategies.push(StrategyInstanceConfig {
            strategy_id: "test-strat".to_string(),
            strategy_type: "nonexistent".to_string(), // invalid type
            exchange: "doesnotexist".to_string(),     // missing exchange
            symbol: "BTC/USDT".to_string(),
            enabled: false, // disabled — should skip all checks
            params: serde_json::json!({}),
        });
        assert!(
            cfg.validate().is_ok(),
            "disabled strategy should skip all validation: {:?}",
            cfg.validate().err(),
        );
    }
}
