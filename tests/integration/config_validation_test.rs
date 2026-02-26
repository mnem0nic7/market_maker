//! Integration tests for TradingConfig loading and validation.
//!
//! These tests exercise the full config pipeline — TOML parsing, default
//! merging, and the `validate()` method — using both the checked-in
//! `config/default.toml` and programmatically created temporary configs.

use std::io::Write;

use rust_decimal_macros::dec;

use trading_core::config::TradingConfig;

// ---------------------------------------------------------------------------
// Helper: load a TradingConfig from an in-memory TOML string
// ---------------------------------------------------------------------------

fn config_from_toml(toml_str: &str) -> anyhow::Result<TradingConfig> {
    let cfg: TradingConfig = toml::from_str(toml_str)?;
    Ok(cfg)
}

// ===========================================================================
// 1. Valid default.toml loads and validates
// ===========================================================================

#[test]
fn test_valid_config_loads_successfully() {
    let toml_str =
        std::fs::read_to_string("config/default.toml").expect("config/default.toml should exist");

    let cfg: TradingConfig =
        toml::from_str(&toml_str).expect("default.toml should deserialize into TradingConfig");

    // The default config has all exchanges disabled and placeholder keys,
    // so it should pass validation (empty keys on disabled exchanges are fine).
    assert!(
        cfg.validate().is_ok(),
        "default.toml should pass validation: {:?}",
        cfg.validate().err(),
    );
}

// ===========================================================================
// 2. Bad config returns descriptive error — zero tick interval
// ===========================================================================

#[test]
fn test_bad_config_zero_tick_returns_descriptive_error() {
    let bad_toml = r#"
[trading_loop]
tick_interval_ms = 0
order_book_stale_ms = 5000

[risk.pnl_limits]
max_daily_loss = "10000"
max_drawdown_pct = "0.05"

[risk.rate_limits]
max_orders_per_second = 10
max_orders_per_minute = 200
"#;

    let cfg = config_from_toml(bad_toml).expect("TOML should parse");
    let err = cfg.validate().unwrap_err();
    let msg = err.to_string();

    assert!(
        msg.contains("tick_interval_ms must be > 0"),
        "error message should mention the bad field: {msg}",
    );
    assert!(
        msg.contains("configuration validation failed"),
        "error message should have the standard prefix: {msg}",
    );
}

// ===========================================================================
// 3. Bad config — enabled exchange with empty credentials
// ===========================================================================

#[test]
fn test_bad_config_enabled_exchange_empty_creds() {
    let bad_toml = r#"
[exchanges.binance]
api_key = ""
secret_key = ""
enabled = true
symbols = ["BTC/USDT"]

[risk.pnl_limits]
max_daily_loss = "10000"
max_drawdown_pct = "0.05"

[risk.rate_limits]
max_orders_per_second = 10
max_orders_per_minute = 200
"#;

    let cfg = config_from_toml(bad_toml).expect("TOML should parse");
    let err = cfg.validate().unwrap_err();
    let msg = err.to_string();

    assert!(
        msg.contains("binance") && msg.contains("api_key is empty"),
        "error should identify the exchange and missing key: {msg}",
    );
    assert!(
        msg.contains("secret_key is empty"),
        "error should also flag the missing secret: {msg}",
    );
}

// ===========================================================================
// 4. Bad config — invalid gateway port
// ===========================================================================

#[test]
fn test_bad_config_invalid_gateway_port() {
    let bad_toml = r#"
[gateway]
enabled = true
host = "0.0.0.0"
port = 80

[risk.pnl_limits]
max_daily_loss = "10000"
max_drawdown_pct = "0.05"

[risk.rate_limits]
max_orders_per_second = 10
max_orders_per_minute = 200
"#;

    let cfg = config_from_toml(bad_toml).expect("TOML should parse");
    let err = cfg.validate().unwrap_err();
    let msg = err.to_string();

    assert!(
        msg.contains("gateway.port must be in range"),
        "error should mention the port range: {msg}",
    );
}

// ===========================================================================
// 5. Bad config — multiple errors collected
// ===========================================================================

#[test]
fn test_bad_config_multiple_errors_collected() {
    let bad_toml = r#"
[trading_loop]
tick_interval_ms = 0
order_book_stale_ms = 0

[risk.pnl_limits]
max_daily_loss = "0"
max_drawdown_pct = "0.05"

[risk.rate_limits]
max_orders_per_second = 0
max_orders_per_minute = 200

[gateway]
enabled = true
port = 42
"#;

    let cfg = config_from_toml(bad_toml).expect("TOML should parse");
    let err = cfg.validate().unwrap_err();
    let msg = err.to_string();

    // Should contain ALL four errors, not just the first one.
    let expected_fragments = [
        "tick_interval_ms",
        "order_book_stale_ms",
        "max_daily_loss must be positive",
        "max_orders_per_second",
        "gateway.port",
    ];
    for frag in &expected_fragments {
        assert!(
            msg.contains(frag),
            "expected error fragment '{frag}' not found in: {msg}",
        );
    }
}

// ===========================================================================
// 6. Config written to a temp file and loaded via the config crate
// ===========================================================================

#[test]
fn test_config_from_temp_file_validates() {
    let valid_toml = r#"
[trading_loop]
tick_interval_ms = 200
order_book_stale_ms = 3000
reconciliation_interval_secs = 0

[risk.pnl_limits]
max_daily_loss = "5000"
max_drawdown_pct = "0.10"

[risk.rate_limits]
max_orders_per_second = 5
max_orders_per_minute = 100

[risk.position_limits]
max_position_per_symbol = "50"
max_gross_exposure = "1000000"

[risk.kill_switch]
max_daily_loss = "5000"
max_position_per_symbol = "200"

[gateway]
enabled = false
"#;

    let dir = tempfile::tempdir().expect("should create temp dir");
    let file_path = dir.path().join("test_config.toml");
    {
        let mut f = std::fs::File::create(&file_path).unwrap();
        f.write_all(valid_toml.as_bytes()).unwrap();
    }

    // Use the `config` crate to load from the temp file (mirrors load_config
    // but with a custom path and no env overlay).
    let settings = config::Config::builder()
        .add_source(config::File::from(file_path))
        .build()
        .expect("config builder should succeed");

    let cfg: TradingConfig = settings
        .try_deserialize()
        .expect("should deserialize into TradingConfig");

    assert!(
        cfg.validate().is_ok(),
        "temp-file config should pass validation: {:?}",
        cfg.validate().err(),
    );
    assert_eq!(cfg.trading_loop.tick_interval_ms, 200);
    assert_eq!(cfg.risk.pnl_limits.max_daily_loss, dec!(5000));
}

// ===========================================================================
// 7. Programmatic config with risk limit inconsistency
// ===========================================================================

#[test]
fn test_bad_config_risk_limit_inconsistency() {
    let mut cfg = TradingConfig::default();
    cfg.risk.pnl_limits.max_daily_loss = dec!(50_000);
    cfg.risk.kill_switch.max_daily_loss = dec!(10_000); // Less than pnl limit

    let err = cfg.validate().unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("kill_switch.max_daily_loss") && msg.contains("less than"),
        "expected risk consistency error, got: {msg}",
    );
}

// ===========================================================================
// 8. Malformed TOML file produces a parse error (not a validation error)
// ===========================================================================

#[test]
fn test_malformed_toml_gives_parse_error() {
    let bad_toml = r#"
[trading_loop
tick_interval_ms = broken
"#;

    let result = config_from_toml(bad_toml);
    assert!(
        result.is_err(),
        "malformed TOML should produce a parse error",
    );
}
