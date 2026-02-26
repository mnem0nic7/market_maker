//! Backtesting CLI binary.
//!
//! Loads historical market events from a JSON lines file (or generates
//! synthetic sample data), creates a strategy, runs the replay engine,
//! and prints the performance report.
//!
//! Configuration is loaded from config files and environment variables,
//! then any CLI arguments override those values.
//!
//! # Examples
//!
//! ```sh
//! # Run with synthetic data (defaults)
//! backtester --strategy spread --synthetic --initial-capital 100000
//!
//! # Run with historical data file
//! backtester --strategy avellaneda --data-file data/btc_trades.jsonl --source jsonl
//!
//! # Run with CSV OHLCV data
//! backtester --strategy grid --data-file data/btc_ohlcv.csv --source csv
//!
//! # Download from Binance and backtest
//! backtester --strategy spread --source binance --binance-symbol ETHUSDT --interval 5m
//!
//! # Parameter sweep / optimization
//! backtester --strategy spread --optimize --param-ranges '{"spread_bps":[5,10,15,20]}'
//!
//! # Strategy-specific parameters via --params
//! backtester --strategy spread --params spread_bps=15,order_size=0.1,num_levels=5
//!
//! # JSON output for scripting
//! backtester --strategy spread --synthetic --output json
//!
//! # CSV output (single-line key metrics)
//! backtester --strategy spread --synthetic --output csv
//! ```

use std::io::BufRead;
use std::path::PathBuf;
use std::str::FromStr;

use anyhow::{Context, Result};
use chrono::{Duration, Utc};
use clap::Parser;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use serde::Deserialize;
use tracing::info;

use mm_backtest::{
    print_report, BacktestConfig, BacktestResult, MarketEvent, OptimizationConfig,
    OptimizationMetric, ParameterOptimizer, ParameterRange, ReplayEngine,
};
use mm_core::traits::Strategy;
use mm_core::{Exchange, PriceLevel, Side, Symbol};
use mm_strategy::{
    AvellanedaStoikovConfig, AvellanedaStoikovStrategy, GridConfig, GridStrategy, GridType,
    MeanReversionConfig, MeanReversionStrategy, MomentumConfig, MomentumStrategy, SpreadConfig,
    SpreadStrategy, StatArbConfig, StatArbStrategy, TwapConfig, TwapStrategy, VwapConfig,
    VwapStrategy,
};

// ---------------------------------------------------------------------------
// CLI Argument Parsing (clap)
// ---------------------------------------------------------------------------

/// Market Maker Backtester - Run historical strategy simulations
///
/// Loads configuration from config files (config/backtester.toml,
/// config/default.toml) and BACKTEST_* environment variables. Any CLI
/// arguments provided will override those values.
///
/// Examples:
///   backtester --strategy spread --synthetic --initial-capital 100000
///   backtester --strategy avellaneda --data-file data/btc.jsonl --source jsonl
///   backtester --strategy grid --optimize --param-ranges '{"spread_bps":[5,10,15]}'
///   backtester --strategy spread --synthetic --output json
///   backtester --strategy spread --params spread_bps=15,order_size=0.1
#[derive(Parser, Debug)]
#[command(name = "backtester", version, about, long_about = None)]
struct Cli {
    /// Strategy type to backtest.
    /// Supported: spread, grid, avellaneda, twap, vwap, momentum, mean_reversion, stat_arb
    #[arg(short, long)]
    strategy: Option<String>,

    /// Trading pair symbol (e.g., BTC/USDT). Format: BASE/QUOTE
    #[arg(long)]
    symbol: Option<String>,

    /// Exchange to simulate
    #[arg(long)]
    exchange: Option<String>,

    /// Initial capital in quote currency
    #[arg(long)]
    initial_capital: Option<f64>,

    /// Path to historical data file (JSONL or CSV)
    #[arg(short, long)]
    data_file: Option<String>,

    /// Data source type: synthetic, csv, binance, jsonl
    #[arg(long)]
    source: Option<String>,

    /// Use synthetic/generated data instead of a file
    #[arg(long)]
    synthetic: bool,

    /// Number of synthetic events to generate
    #[arg(long)]
    num_events: Option<usize>,

    /// Binance symbol for download (e.g., BTCUSDT). Only with --source binance
    #[arg(long)]
    binance_symbol: Option<String>,

    /// Kline interval for Binance download (e.g., 1m, 5m, 1h)
    #[arg(long)]
    interval: Option<String>,

    /// Fee rate as a decimal (e.g., 0.001 = 10 bps)
    #[arg(long)]
    fee_rate: Option<f64>,

    /// Maker fee in basis points (alternative to --fee-rate; converted to decimal)
    #[arg(long)]
    maker_fee_bps: Option<u32>,

    /// Taker fee in basis points (sets fee_rate; converted to decimal)
    #[arg(long)]
    taker_fee_bps: Option<u32>,

    /// Slippage in basis points
    #[arg(long)]
    slippage_bps: Option<f64>,

    /// Spread in basis points (spread strategy)
    #[arg(long)]
    spread_bps: Option<f64>,

    /// Order size per level
    #[arg(long)]
    order_size: Option<f64>,

    /// Number of price levels per side
    #[arg(long)]
    num_levels: Option<usize>,

    /// Grid spacing in price units (grid strategy)
    #[arg(long)]
    grid_spacing: Option<f64>,

    /// Risk aversion parameter gamma (Avellaneda-Stoikov strategy)
    #[arg(long)]
    gamma: Option<f64>,

    /// Volatility estimate sigma (Avellaneda-Stoikov strategy)
    #[arg(long)]
    sigma: Option<f64>,

    /// Output format: text, json, csv
    #[arg(short, long, default_value = "text")]
    output: String,

    /// Run parameter optimization (grid search)
    #[arg(long)]
    optimize: bool,

    /// JSON string specifying parameter ranges for optimization.
    /// Example: '{"spread_bps":[5,10,15,20], "num_levels":[2,3,5]}'
    #[arg(long)]
    param_ranges: Option<String>,

    /// Optimization metric: sharpe, sortino, total_pnl, max_drawdown, profit_factor, win_rate
    #[arg(long)]
    metric: Option<String>,

    /// Number of top optimization results to display
    #[arg(long)]
    top_n: Option<usize>,

    /// Config file path (overrides default config file locations)
    #[arg(short, long)]
    config: Option<String>,

    /// Strategy-specific parameters as key=value pairs (comma-separated).
    /// Example: --params spread_bps=15,order_size=0.1,num_levels=5
    #[arg(long, value_delimiter = ',')]
    params: Vec<String>,
}

// ---------------------------------------------------------------------------
// Configuration (file/env based, overridden by CLI)
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct CliConfig {
    /// Path to a JSON lines file containing MarketEvent records.
    /// If empty or missing, synthetic sample data is generated.
    #[serde(default)]
    data_file: String,

    /// Data source: "synthetic" (default), "csv", "binance", or "jsonl".
    /// - "synthetic": generate random walk data
    /// - "csv": load OHLCV from data_file
    /// - "binance": download klines from Binance API
    /// - "jsonl": load MarketEvent JSON lines from data_file (legacy)
    #[serde(default = "default_source")]
    source: String,

    /// Binance symbol for download (e.g. "BTCUSDT"). Only used with source=binance.
    #[serde(default)]
    binance_symbol: String,

    /// Kline interval for Binance download (e.g. "1m", "5m", "1h").
    #[serde(default = "default_interval")]
    interval: String,

    /// Strategy type: "spread", "grid", or "avellaneda".
    #[serde(default = "default_strategy_type")]
    strategy_type: String,

    /// Initial capital in quote currency.
    #[serde(default = "default_initial_capital")]
    initial_capital: Decimal,

    /// Fee rate (e.g. 0.001 = 10 bps).
    #[serde(default = "default_fee_rate")]
    fee_rate: Decimal,

    /// Slippage in basis points.
    #[serde(default)]
    slippage_bps: Decimal,

    /// Number of synthetic events to generate if no data file is provided.
    #[serde(default = "default_num_events")]
    num_events: usize,

    // -- Strategy-specific parameters --
    /// Spread in basis points (for spread strategy).
    #[serde(default = "default_spread_bps")]
    spread_bps: Decimal,

    /// Order size per level.
    #[serde(default = "default_order_size")]
    order_size: Decimal,

    /// Number of price levels per side.
    #[serde(default = "default_num_levels")]
    num_levels: usize,

    /// Grid spacing (for grid strategy).
    #[serde(default = "default_grid_spacing")]
    grid_spacing: Decimal,

    /// Risk aversion parameter gamma (for Avellaneda-Stoikov).
    #[serde(default = "default_gamma")]
    gamma: Decimal,

    /// Volatility estimate sigma (for Avellaneda-Stoikov).
    #[serde(default = "default_sigma")]
    sigma: Decimal,

    // -- Optimizer parameters --
    /// Enable parameter optimisation mode (grid search).
    #[serde(default)]
    optimize: bool,

    /// JSON string specifying parameter ranges for optimisation.
    /// Example: `{"spread_bps": [5, 10, 15, 20], "num_levels": [2, 3, 5]}`
    #[serde(default)]
    param_ranges: String,

    /// Optimisation metric: "sharpe", "sortino", "total_pnl", "max_drawdown",
    /// "profit_factor", or "win_rate".
    #[serde(default = "default_metric")]
    metric: String,

    /// Number of top results to display.
    #[serde(default = "default_top_n")]
    top_n: usize,

    /// Output format: "text" (default), "json", or "csv".
    #[serde(default = "default_output_format")]
    output_format: String,
}

fn default_source() -> String {
    "synthetic".to_string()
}
fn default_interval() -> String {
    "1m".to_string()
}
fn default_strategy_type() -> String {
    "spread".to_string()
}
fn default_initial_capital() -> Decimal {
    dec!(100_000)
}
fn default_fee_rate() -> Decimal {
    dec!(0.001)
}
fn default_num_events() -> usize {
    1000
}
fn default_spread_bps() -> Decimal {
    dec!(10)
}
fn default_order_size() -> Decimal {
    dec!(0.01)
}
fn default_num_levels() -> usize {
    3
}
fn default_grid_spacing() -> Decimal {
    dec!(50)
}
fn default_gamma() -> Decimal {
    dec!(0.1)
}
fn default_sigma() -> Decimal {
    dec!(0.02)
}
fn default_metric() -> String {
    "sharpe".to_string()
}
fn default_top_n() -> usize {
    10
}
fn default_output_format() -> String {
    "text".to_string()
}

impl Default for CliConfig {
    fn default() -> Self {
        Self {
            data_file: String::new(),
            source: default_source(),
            binance_symbol: String::new(),
            interval: default_interval(),
            strategy_type: default_strategy_type(),
            initial_capital: default_initial_capital(),
            fee_rate: default_fee_rate(),
            slippage_bps: Decimal::ZERO,
            num_events: default_num_events(),
            spread_bps: default_spread_bps(),
            order_size: default_order_size(),
            num_levels: default_num_levels(),
            grid_spacing: default_grid_spacing(),
            gamma: default_gamma(),
            sigma: default_sigma(),
            optimize: false,
            param_ranges: String::new(),
            metric: default_metric(),
            top_n: default_top_n(),
            output_format: default_output_format(),
        }
    }
}

// ---------------------------------------------------------------------------
// CLI -> Config merging
// ---------------------------------------------------------------------------

/// Parse strategy-specific --params key=value pairs and apply them to the config.
fn apply_params(cfg: &mut CliConfig, params: &[String]) {
    for param in params {
        let param = param.trim();
        if param.is_empty() {
            continue;
        }
        if let Some((key, value)) = param.split_once('=') {
            let key = key.trim();
            let value = value.trim();
            match key {
                "spread_bps" => {
                    if let Ok(v) = Decimal::from_str(value) {
                        cfg.spread_bps = v;
                    }
                }
                "order_size" => {
                    if let Ok(v) = Decimal::from_str(value) {
                        cfg.order_size = v;
                    }
                }
                "num_levels" => {
                    if let Ok(v) = value.parse::<usize>() {
                        cfg.num_levels = v;
                    }
                }
                "grid_spacing" => {
                    if let Ok(v) = Decimal::from_str(value) {
                        cfg.grid_spacing = v;
                    }
                }
                "gamma" => {
                    if let Ok(v) = Decimal::from_str(value) {
                        cfg.gamma = v;
                    }
                }
                "sigma" => {
                    if let Ok(v) = Decimal::from_str(value) {
                        cfg.sigma = v;
                    }
                }
                "fee_rate" => {
                    if let Ok(v) = Decimal::from_str(value) {
                        cfg.fee_rate = v;
                    }
                }
                "slippage_bps" => {
                    if let Ok(v) = Decimal::from_str(value) {
                        cfg.slippage_bps = v;
                    }
                }
                "initial_capital" => {
                    if let Ok(v) = Decimal::from_str(value) {
                        cfg.initial_capital = v;
                    }
                }
                "num_events" => {
                    if let Ok(v) = value.parse::<usize>() {
                        cfg.num_events = v;
                    }
                }
                _ => {
                    tracing::warn!(key, value, "unknown --params key, ignoring");
                }
            }
        } else {
            tracing::warn!(param, "invalid --params format, expected key=value");
        }
    }
}

/// Convert an f64 to Decimal via its string representation for precision.
/// This avoids the imprecision of `Decimal::from_f64_retain` for values
/// like 0.0005 that cannot be represented exactly in binary floating point.
fn decimal_from_f64(v: f64) -> Option<Decimal> {
    Decimal::from_str(&v.to_string()).ok()
}

/// Apply CLI arguments on top of the config-file-loaded CliConfig.
/// Only overrides values that were explicitly provided on the command line.
fn apply_cli_overrides(cfg: &mut CliConfig, cli: &Cli) {
    if let Some(ref s) = cli.strategy {
        cfg.strategy_type = s.clone();
    }
    if let Some(ref s) = cli.source {
        cfg.source = s.clone();
    }
    if cli.synthetic {
        cfg.source = "synthetic".to_string();
        // Clear data_file when --synthetic is used, unless --data-file also given
        if cli.data_file.is_none() {
            cfg.data_file.clear();
        }
    }
    if let Some(ref f) = cli.data_file {
        cfg.data_file = f.clone();
    }
    if let Some(v) = cli.initial_capital {
        cfg.initial_capital = decimal_from_f64(v).unwrap_or(cfg.initial_capital);
    }
    if let Some(v) = cli.fee_rate {
        cfg.fee_rate = decimal_from_f64(v).unwrap_or(cfg.fee_rate);
    }
    // --maker-fee-bps and --taker-fee-bps convert basis points to decimal fee rate
    if let Some(bps) = cli.maker_fee_bps {
        cfg.fee_rate = Decimal::from(bps) / dec!(10000);
    }
    if let Some(bps) = cli.taker_fee_bps {
        cfg.fee_rate = Decimal::from(bps) / dec!(10000);
    }
    if let Some(v) = cli.slippage_bps {
        cfg.slippage_bps = decimal_from_f64(v).unwrap_or(cfg.slippage_bps);
    }
    if let Some(v) = cli.num_events {
        cfg.num_events = v;
    }
    if let Some(ref s) = cli.binance_symbol {
        cfg.binance_symbol = s.clone();
    }
    if let Some(ref s) = cli.interval {
        cfg.interval = s.clone();
    }
    if let Some(v) = cli.spread_bps {
        cfg.spread_bps = decimal_from_f64(v).unwrap_or(cfg.spread_bps);
    }
    if let Some(v) = cli.order_size {
        cfg.order_size = decimal_from_f64(v).unwrap_or(cfg.order_size);
    }
    if let Some(v) = cli.num_levels {
        cfg.num_levels = v;
    }
    if let Some(v) = cli.grid_spacing {
        cfg.grid_spacing = decimal_from_f64(v).unwrap_or(cfg.grid_spacing);
    }
    if let Some(v) = cli.gamma {
        cfg.gamma = decimal_from_f64(v).unwrap_or(cfg.gamma);
    }
    if let Some(v) = cli.sigma {
        cfg.sigma = decimal_from_f64(v).unwrap_or(cfg.sigma);
    }
    if cli.optimize {
        cfg.optimize = true;
    }
    if let Some(ref s) = cli.param_ranges {
        cfg.param_ranges = s.clone();
    }
    if let Some(ref s) = cli.metric {
        cfg.metric = s.clone();
    }
    if let Some(v) = cli.top_n {
        cfg.top_n = v;
    }

    // Output format: CLI --output overrides config (default is "text")
    cfg.output_format = cli.output.clone();

    // Apply --params key=value overrides last (highest priority)
    if !cli.params.is_empty() {
        apply_params(cfg, &cli.params);
    }
}

// ---------------------------------------------------------------------------
// Output formatting
// ---------------------------------------------------------------------------

/// Output the backtest result in the requested format.
fn output_result(result: &BacktestResult, format: &str) {
    match format {
        "json" => match serde_json::to_string_pretty(result) {
            Ok(json) => println!("{}", json),
            Err(e) => eprintln!("Failed to serialize result to JSON: {}", e),
        },
        "csv" => {
            // Print CSV header and a single data row with key metrics
            println!(
                "net_pnl,total_pnl,total_fees,num_trades,num_fills,win_rate,\
                 max_drawdown,max_drawdown_pct,sharpe_ratio,sortino_ratio,\
                 avg_trade_pnl,profit_factor,total_volume"
            );
            println!(
                "{},{},{},{},{},{},{},{},{},{},{},{},{}",
                result.net_pnl,
                result.total_pnl,
                result.total_fees,
                result.num_trades,
                result.num_fills,
                result.win_rate,
                result.max_drawdown,
                result.max_drawdown_pct,
                result.sharpe_ratio,
                result.sortino_ratio,
                result.avg_trade_pnl,
                result.profit_factor,
                result.total_volume,
            );
        }
        _ => {
            // "text" or any unrecognized format: use the existing human-readable report
            print_report(result);
        }
    }
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing.
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .with_target(false)
        .init();

    info!("backtester starting");

    // 1. Parse CLI arguments
    let cli = Cli::parse();

    // 2. Load base configuration from files + env vars
    let mut cfg = load_config(cli.config.as_deref()).context("failed to load configuration")?;

    // 3. Override config with CLI arguments
    apply_cli_overrides(&mut cfg, &cli);

    // Validate output format
    let valid_formats = ["text", "json", "csv"];
    if !valid_formats.contains(&cfg.output_format.as_str()) {
        anyhow::bail!(
            "invalid output format: '{}'. Supported: text, json, csv",
            cfg.output_format
        );
    }

    info!(
        strategy = %cfg.strategy_type,
        initial_capital = %cfg.initial_capital,
        fee_rate = %cfg.fee_rate,
        slippage_bps = %cfg.slippage_bps,
        output_format = %cfg.output_format,
        "configuration loaded"
    );

    // ------------------------------------------------------------------
    // 1. Load or generate market events
    // ------------------------------------------------------------------
    let events = match cfg.source.to_lowercase().as_str() {
        "csv" => {
            let path = PathBuf::from(&cfg.data_file);
            info!(path = %path.display(), "loading CSV OHLCV data");
            mm_backtest::load_csv(
                &path,
                Exchange::Simulated,
                Symbol::new("BTC", "USDT"),
                cfg.spread_bps,
            )
            .context("failed to load CSV data")?
        }
        "binance" => {
            let binance_sym = if cfg.binance_symbol.is_empty() {
                "BTCUSDT".to_string()
            } else {
                cfg.binance_symbol.clone()
            };
            let end = Utc::now().timestamp_millis();
            // Default to last 24 hours of data.
            let start = end - 24 * 60 * 60 * 1000;
            info!(
                symbol = %binance_sym,
                interval = %cfg.interval,
                "downloading Binance klines"
            );
            mm_backtest::download_binance_klines(
                &binance_sym,
                &cfg.interval,
                start,
                end,
                Symbol::new("BTC", "USDT"),
                cfg.spread_bps,
            )
            .await
            .context("failed to download Binance klines")?
        }
        "jsonl" => {
            let path = PathBuf::from(&cfg.data_file);
            info!(path = %path.display(), "loading market events from JSONL file");
            load_events_from_file(&path).context("failed to load market events")?
        }
        _ => {
            // "synthetic" or empty/unknown -> generate
            if !cfg.data_file.is_empty() {
                // Legacy behavior: if data_file is set but source is synthetic, try JSONL
                let path = PathBuf::from(&cfg.data_file);
                info!(path = %path.display(), "loading market events from file");
                load_events_from_file(&path).context("failed to load market events")?
            } else {
                info!(
                    num_events = cfg.num_events,
                    "generating synthetic market data"
                );
                generate_sample_data(cfg.num_events)
            }
        }
    };

    info!(total_events = events.len(), "market events ready");

    // ------------------------------------------------------------------
    // 2. Optimisation mode or single backtest
    // ------------------------------------------------------------------
    if cfg.optimize {
        run_optimization(&cfg, events)?;
    } else {
        run_single_backtest(&cfg, events)?;
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Single backtest
// ---------------------------------------------------------------------------

fn run_single_backtest(cfg: &CliConfig, events: Vec<MarketEvent>) -> Result<()> {
    let mut strategy: Box<dyn Strategy> = create_strategy(cfg)?;
    info!(
        strategy_type = %cfg.strategy_type,
        strategy_name = strategy.name(),
        "strategy created"
    );

    let backtest_config = BacktestConfig {
        initial_capital: cfg.initial_capital,
        fee_rate: cfg.fee_rate,
        slippage_bps: cfg.slippage_bps,
        start_time: None,
        end_time: None,
    };

    let engine = ReplayEngine::new(backtest_config);
    info!("running backtest...");

    let result = engine.run(strategy.as_mut(), events);

    // Output in the requested format
    output_result(&result, &cfg.output_format);

    info!(
        net_pnl = %result.net_pnl,
        total_pnl = %result.total_pnl,
        total_fees = %result.total_fees,
        num_trades = result.num_trades,
        num_fills = result.num_fills,
        win_rate = %result.win_rate,
        max_drawdown = %result.max_drawdown,
        sharpe_ratio = %result.sharpe_ratio,
        "backtest complete"
    );

    Ok(())
}

// ---------------------------------------------------------------------------
// Optimisation mode
// ---------------------------------------------------------------------------

fn parse_metric(s: &str) -> OptimizationMetric {
    match s.to_lowercase().as_str() {
        "sortino" => OptimizationMetric::Sortino,
        "total_pnl" | "pnl" => OptimizationMetric::TotalPnl,
        "max_drawdown" | "drawdown" => OptimizationMetric::MaxDrawdown,
        "profit_factor" => OptimizationMetric::ProfitFactor,
        "win_rate" => OptimizationMetric::WinRate,
        _ => OptimizationMetric::Sharpe,
    }
}

fn run_optimization(cfg: &CliConfig, events: Vec<MarketEvent>) -> Result<()> {
    // Parse param_ranges JSON
    let param_ranges: Vec<ParameterRange> = if cfg.param_ranges.is_empty() {
        // Default: sweep spread_bps if no ranges specified
        info!("No --param-ranges specified, using default spread_bps sweep");
        vec![ParameterRange {
            name: "spread_bps".to_string(),
            values: vec![
                serde_json::json!(5),
                serde_json::json!(10),
                serde_json::json!(15),
                serde_json::json!(20),
            ],
        }]
    } else {
        // Parse JSON object: {"param_name": [val1, val2, ...], ...}
        let raw: serde_json::Map<String, serde_json::Value> =
            serde_json::from_str(&cfg.param_ranges)
                .context("failed to parse --param-ranges JSON")?;

        raw.into_iter()
            .map(|(name, values)| {
                let vals = match values {
                    serde_json::Value::Array(arr) => arr,
                    other => vec![other],
                };
                ParameterRange { name, values: vals }
            })
            .collect()
    };

    let metric = parse_metric(&cfg.metric);

    // Build base_params from CLI config values
    let base_params = serde_json::json!({
        "spread_bps": cfg.spread_bps.to_string(),
        "order_size": cfg.order_size.to_string(),
        "num_levels": cfg.num_levels,
        "grid_spacing": cfg.grid_spacing.to_string(),
        "gamma": cfg.gamma.to_string(),
        "sigma": cfg.sigma.to_string(),
    });

    let opt_config = OptimizationConfig {
        strategy_type: cfg.strategy_type.clone(),
        base_params,
        param_ranges,
        backtest_config: BacktestConfig {
            initial_capital: cfg.initial_capital,
            fee_rate: cfg.fee_rate,
            slippage_bps: cfg.slippage_bps,
            start_time: None,
            end_time: None,
        },
        metric,
        top_n: cfg.top_n,
    };

    info!(
        strategy_type = %cfg.strategy_type,
        metric = ?metric,
        top_n = cfg.top_n,
        "starting parameter optimisation"
    );

    let results = ParameterOptimizer::run(&opt_config, events, None);

    // Print results table
    println!();
    println!("============================================================");
    println!(
        "              OPTIMISATION RESULTS ({:?})               ",
        metric
    );
    println!("============================================================");
    println!();
    println!(
        "{:<5} {:<12} {:<12} {:<12} {:<12} {:<12}  Params",
        "Rank", "Metric", "Net PnL", "Sharpe", "Win Rate", "Max DD"
    );
    println!("{}", "-".repeat(100));

    for (i, res) in results.iter().enumerate() {
        let br = &res.backtest_result;
        // Format params as compact key=value pairs
        let param_str = if let Some(obj) = res.params.as_object() {
            obj.iter()
                .filter(|(k, _)| {
                    // Only show the swept parameters, not defaults
                    !["strategy_id", "exchange", "symbol"].contains(&k.as_str())
                })
                .map(|(k, v)| format!("{}={}", k, v))
                .collect::<Vec<_>>()
                .join(", ")
        } else {
            format!("{}", res.params)
        };

        println!(
            "{:<5} {:<12.4} {:<12.2} {:<12.4} {:<12.2}% {:<12.2}  {}",
            i + 1,
            res.metric_value,
            br.net_pnl,
            br.sharpe_ratio,
            br.win_rate * dec!(100),
            br.max_drawdown,
            param_str,
        );
    }

    println!();
    println!("============================================================");
    println!();

    Ok(())
}

// ---------------------------------------------------------------------------
// Strategy factory
// ---------------------------------------------------------------------------

fn create_strategy(cfg: &CliConfig) -> Result<Box<dyn Strategy>> {
    let exchange = Exchange::Simulated;
    let symbol = Symbol::new("BTC", "USDT");

    match cfg.strategy_type.to_lowercase().as_str() {
        "spread" => {
            let spread_config = SpreadConfig {
                strategy_id: "backtest-spread".to_string(),
                exchange,
                symbol,
                spread_bps: cfg.spread_bps,
                order_size: cfg.order_size,
                num_levels: cfg.num_levels,
                refresh_interval_ms: 0, // No throttle in backtest
                min_spread_bps: cfg.spread_bps / dec!(2),
                reprice_threshold_bps: cfg.spread_bps,
            };
            Ok(Box::new(SpreadStrategy::new(spread_config)))
        }
        "grid" => {
            // Compute grid bounds: center around 50000 with grid_spacing * num_levels range
            let half_range = cfg.grid_spacing * Decimal::from(cfg.num_levels as u64);
            let center = dec!(50000);
            let grid_config = GridConfig {
                strategy_id: "backtest-grid".to_string(),
                exchange,
                symbol,
                grid_type: GridType::Arithmetic,
                num_grids: cfg.num_levels,
                order_size: cfg.order_size,
                upper_price: center + half_range,
                lower_price: center - half_range,
            };
            Ok(Box::new(GridStrategy::new(grid_config)))
        }
        "avellaneda" => {
            let as_config = AvellanedaStoikovConfig {
                strategy_id: "backtest-avellaneda".to_string(),
                exchange,
                symbol,
                gamma: cfg.gamma,
                sigma: cfg.sigma,
                kappa: dec!(1.5),
                order_size: cfg.order_size,
                max_position: cfg.order_size * Decimal::from(cfg.num_levels as u64 * 2),
                session_duration_secs: 3600,
            };
            Ok(Box::new(AvellanedaStoikovStrategy::new(as_config)))
        }
        "twap" => {
            let twap_config = TwapConfig {
                strategy_id: "backtest-twap".to_string(),
                exchange,
                symbol,
                target_quantity: cfg.order_size * Decimal::from(cfg.num_levels as u64 * 10),
                duration_secs: 3600,
                slice_interval_secs: 60,
                side: Side::Buy,
                max_deviation_bps: cfg.spread_bps,
            };
            Ok(Box::new(TwapStrategy::new(twap_config)))
        }
        "vwap" => {
            let vwap_config = VwapConfig {
                strategy_id: "backtest-vwap".to_string(),
                exchange,
                symbol,
                target_quantity: cfg.order_size * Decimal::from(cfg.num_levels as u64 * 10),
                duration_secs: 3600,
                side: Side::Buy,
                volume_profile: vec![dec!(0.1), dec!(0.2), dec!(0.3), dec!(0.25), dec!(0.15)],
                max_deviation_bps: cfg.spread_bps,
            };
            Ok(Box::new(VwapStrategy::new(vwap_config)))
        }
        "momentum" => {
            let momentum_config = MomentumConfig {
                strategy_id: "backtest-momentum".to_string(),
                exchange,
                symbol,
                lookback_periods: cfg.num_levels.max(2),
                entry_threshold_bps: cfg.spread_bps * dec!(5),
                exit_threshold_bps: cfg.spread_bps,
                order_size: cfg.order_size,
                trailing_stop_bps: cfg.spread_bps * dec!(3),
                max_position: cfg.order_size * Decimal::from(cfg.num_levels as u64 * 2),
            };
            Ok(Box::new(MomentumStrategy::new(momentum_config)))
        }
        "mean_reversion" => {
            let mr_config = MeanReversionConfig {
                strategy_id: "backtest-mean-reversion".to_string(),
                exchange,
                symbol,
                window_periods: cfg.num_levels.max(2),
                num_std_dev: dec!(2),
                order_size: cfg.order_size,
                max_position: cfg.order_size * Decimal::from(cfg.num_levels as u64 * 2),
                exit_at_mean: true,
            };
            Ok(Box::new(MeanReversionStrategy::new(mr_config)))
        }
        "stat_arb" => {
            let sa_config = StatArbConfig {
                strategy_id: "backtest-stat-arb".to_string(),
                leg_a_exchange: exchange,
                leg_a_symbol: symbol.clone(),
                leg_b_exchange: exchange,
                leg_b_symbol: symbol,
                lookback_periods: cfg.num_levels.max(2),
                entry_z_score: dec!(2),
                exit_z_score: dec!(0.5),
                order_size: cfg.order_size,
                max_position: cfg.order_size * Decimal::from(cfg.num_levels as u64 * 2),
            };
            Ok(Box::new(StatArbStrategy::new(sa_config)))
        }
        other => {
            anyhow::bail!(
                "unknown strategy type: '{}'. Supported: spread, grid, avellaneda, twap, vwap, momentum, mean_reversion, stat_arb",
                other
            );
        }
    }
}

// ---------------------------------------------------------------------------
// Event loading
// ---------------------------------------------------------------------------

/// Load market events from a JSON lines file. Each line should be a
/// serialized [`MarketEvent`].
fn load_events_from_file(path: &PathBuf) -> Result<Vec<MarketEvent>> {
    let file = std::fs::File::open(path)
        .with_context(|| format!("failed to open data file: {}", path.display()))?;
    let reader = std::io::BufReader::new(file);
    let mut events = Vec::new();

    for (line_no, line) in reader.lines().enumerate() {
        let line = line.with_context(|| format!("failed to read line {}", line_no + 1))?;
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let event: MarketEvent = serde_json::from_str(trimmed)
            .with_context(|| format!("failed to parse MarketEvent on line {}", line_no + 1))?;
        events.push(event);
    }

    Ok(events)
}

// ---------------------------------------------------------------------------
// Synthetic data generation
// ---------------------------------------------------------------------------

/// Generate synthetic market events using a random walk for order book
/// snapshots. This produces a plausible sequence of order book updates
/// suitable for testing strategies.
fn generate_sample_data(num_events: usize) -> Vec<MarketEvent> {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    let mut events = Vec::with_capacity(num_events);
    let exchange = Exchange::Simulated;
    let symbol = Symbol::new("BTC", "USDT");
    let base_time = Utc::now() - Duration::seconds(num_events as i64);

    // Starting mid price
    let mut mid_price: f64 = 50_000.0;
    let tick_size: f64 = 0.01;

    // Simple deterministic "random" walk using hash-based pseudo-randomness.
    // This avoids pulling in the rand crate while still producing varied data.
    let mut seed: u64 = 42;

    for i in 0..num_events {
        let ts = base_time + Duration::seconds(i as i64);

        // Pseudo-random step: hash the seed to get a new value
        seed = {
            let mut hasher = DefaultHasher::new();
            seed.hash(&mut hasher);
            i.hash(&mut hasher);
            hasher.finish()
        };

        // Map to a step in [-0.5, 0.5] % of mid price
        let step_pct = ((seed % 1000) as f64 / 1000.0 - 0.5) * 0.005;
        mid_price *= 1.0 + step_pct;
        mid_price = (mid_price / tick_size).round() * tick_size;

        // Ensure price stays positive
        if mid_price < 100.0 {
            mid_price = 100.0;
        }

        // Generate a spread between 1-10 bps
        let spread_bps = 1.0 + (seed % 10) as f64;
        let half_spread = mid_price * spread_bps / 10_000.0 / 2.0;

        let best_bid = mid_price - half_spread;
        let best_ask = mid_price + half_spread;

        // Build 5 levels of depth on each side
        let num_levels = 5;
        let level_step = half_spread * 0.5; // each level 50% of half-spread apart

        let bids: Vec<PriceLevel> = (0..num_levels)
            .map(|j| {
                let price = best_bid - (j as f64 * level_step);
                let qty = 0.1 + (seed.wrapping_add(j as u64) % 100) as f64 / 100.0;
                PriceLevel {
                    price: Decimal::from_f64_retain(price)
                        .unwrap_or(dec!(50000))
                        .round_dp(2),
                    quantity: Decimal::from_f64_retain(qty)
                        .unwrap_or(dec!(0.1))
                        .round_dp(4),
                }
            })
            .collect();

        let asks: Vec<PriceLevel> = (0..num_levels)
            .map(|j| {
                let price = best_ask + (j as f64 * level_step);
                let qty = 0.1 + (seed.wrapping_add(j as u64 + 100) % 100) as f64 / 100.0;
                PriceLevel {
                    price: Decimal::from_f64_retain(price)
                        .unwrap_or(dec!(50000))
                        .round_dp(2),
                    quantity: Decimal::from_f64_retain(qty)
                        .unwrap_or(dec!(0.1))
                        .round_dp(4),
                }
            })
            .collect();

        // Alternate between order book snapshots and trade events
        if i % 3 == 0 && i > 0 {
            // Trade event
            let trade_side = if seed.is_multiple_of(2) {
                Side::Buy
            } else {
                Side::Sell
            };
            let trade_price = if trade_side == Side::Buy {
                best_ask
            } else {
                best_bid
            };
            let trade_qty = 0.01 + (seed % 50) as f64 / 1000.0;

            events.push(MarketEvent::TradeEvent {
                timestamp: ts,
                exchange,
                symbol: symbol.clone(),
                price: Decimal::from_f64_retain(trade_price)
                    .unwrap_or(dec!(50000))
                    .round_dp(2),
                quantity: Decimal::from_f64_retain(trade_qty)
                    .unwrap_or(dec!(0.01))
                    .round_dp(6),
                side: trade_side,
                trade_id: format!("synth-{}", i),
            });
        } else {
            // Order book snapshot
            events.push(MarketEvent::OrderBookSnapshot {
                timestamp: ts,
                exchange,
                symbol: symbol.clone(),
                bids,
                asks,
                sequence: i as u64,
            });
        }
    }

    events
}

// ---------------------------------------------------------------------------
// Configuration loading
// ---------------------------------------------------------------------------

fn load_config(config_path: Option<&str>) -> Result<CliConfig> {
    let mut builder = config::Config::builder()
        // Defaults
        .set_default("data_file", "")?
        .set_default("source", "synthetic")?
        .set_default("binance_symbol", "")?
        .set_default("interval", "1m")?
        .set_default("strategy_type", "spread")?
        .set_default("initial_capital", "100000")?
        .set_default("fee_rate", "0.001")?
        .set_default("slippage_bps", "0")?
        .set_default("num_events", 1000_i64)?
        .set_default("spread_bps", "10")?
        .set_default("order_size", "0.01")?
        .set_default("num_levels", 3_i64)?
        .set_default("grid_spacing", "50")?
        .set_default("gamma", "0.1")?
        .set_default("sigma", "0.02")?
        .set_default("optimize", false)?
        .set_default("param_ranges", "")?
        .set_default("metric", "sharpe")?
        .set_default("top_n", 10_i64)?
        .set_default("output_format", "text")?;

    // Config files: if --config is specified, use that; otherwise use defaults
    if let Some(path) = config_path {
        builder = builder.add_source(config::File::with_name(path).required(true));
    } else {
        builder = builder
            .add_source(config::File::with_name("config/backtester").required(false))
            .add_source(config::File::with_name("config/default").required(false));
    }

    // Environment variables (prefixed with BACKTEST_)
    let settings = builder
        .add_source(config::Environment::with_prefix("BACKTEST").separator("_"))
        .build()
        .context("failed to build config")?;

    settings
        .try_deserialize::<CliConfig>()
        .context("failed to deserialize config")
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    // -----------------------------------------------------------------------
    // CLI parsing tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_cli_default_values() {
        // No arguments: all Options should be None, defaults should apply
        let cli = Cli::try_parse_from(["backtester"]).unwrap();
        assert!(cli.strategy.is_none());
        assert!(cli.data_file.is_none());
        assert!(cli.initial_capital.is_none());
        assert!(!cli.synthetic);
        assert!(!cli.optimize);
        assert_eq!(cli.output, "text");
        assert!(cli.params.is_empty());
    }

    #[test]
    fn test_cli_strategy_and_synthetic() {
        let cli =
            Cli::try_parse_from(["backtester", "--strategy", "avellaneda", "--synthetic"]).unwrap();
        assert_eq!(cli.strategy.as_deref(), Some("avellaneda"));
        assert!(cli.synthetic);
    }

    #[test]
    fn test_cli_data_file_and_source() {
        let cli = Cli::try_parse_from([
            "backtester",
            "--data-file",
            "data/btc.jsonl",
            "--source",
            "jsonl",
        ])
        .unwrap();
        assert_eq!(cli.data_file.as_deref(), Some("data/btc.jsonl"));
        assert_eq!(cli.source.as_deref(), Some("jsonl"));
    }

    #[test]
    fn test_cli_initial_capital() {
        let cli = Cli::try_parse_from(["backtester", "--initial-capital", "50000.5"]).unwrap();
        assert_eq!(cli.initial_capital, Some(50000.5));
    }

    #[test]
    fn test_cli_fee_bps() {
        let cli = Cli::try_parse_from([
            "backtester",
            "--maker-fee-bps",
            "10",
            "--taker-fee-bps",
            "15",
        ])
        .unwrap();
        assert_eq!(cli.maker_fee_bps, Some(10));
        assert_eq!(cli.taker_fee_bps, Some(15));
    }

    #[test]
    fn test_cli_output_format() {
        let cli = Cli::try_parse_from(["backtester", "--output", "json"]).unwrap();
        assert_eq!(cli.output, "json");

        let cli = Cli::try_parse_from(["backtester", "--output", "csv"]).unwrap();
        assert_eq!(cli.output, "csv");

        let cli = Cli::try_parse_from(["backtester", "-o", "json"]).unwrap();
        assert_eq!(cli.output, "json");
    }

    #[test]
    fn test_cli_optimize_with_params() {
        let cli = Cli::try_parse_from([
            "backtester",
            "--optimize",
            "--param-ranges",
            r#"{"spread_bps":[5,10,15]}"#,
            "--metric",
            "sharpe",
            "--top-n",
            "5",
        ])
        .unwrap();
        assert!(cli.optimize);
        assert_eq!(
            cli.param_ranges.as_deref(),
            Some(r#"{"spread_bps":[5,10,15]}"#)
        );
        assert_eq!(cli.metric.as_deref(), Some("sharpe"));
        assert_eq!(cli.top_n, Some(5));
    }

    #[test]
    fn test_cli_params_key_value() {
        let cli = Cli::try_parse_from([
            "backtester",
            "--params",
            "spread_bps=15,order_size=0.1,num_levels=5",
        ])
        .unwrap();
        assert_eq!(cli.params.len(), 3);
        assert_eq!(cli.params[0], "spread_bps=15");
        assert_eq!(cli.params[1], "order_size=0.1");
        assert_eq!(cli.params[2], "num_levels=5");
    }

    #[test]
    fn test_cli_config_file_override() {
        let cli = Cli::try_parse_from(["backtester", "--config", "/tmp/my_config.toml"]).unwrap();
        assert_eq!(cli.config.as_deref(), Some("/tmp/my_config.toml"));
    }

    #[test]
    fn test_cli_strategy_specific_args() {
        let cli = Cli::try_parse_from([
            "backtester",
            "--spread-bps",
            "20",
            "--order-size",
            "0.05",
            "--num-levels",
            "7",
            "--grid-spacing",
            "100",
            "--gamma",
            "0.3",
            "--sigma",
            "0.05",
        ])
        .unwrap();
        assert_eq!(cli.spread_bps, Some(20.0));
        assert_eq!(cli.order_size, Some(0.05));
        assert_eq!(cli.num_levels, Some(7));
        assert_eq!(cli.grid_spacing, Some(100.0));
        assert_eq!(cli.gamma, Some(0.3));
        assert_eq!(cli.sigma, Some(0.05));
    }

    #[test]
    fn test_cli_short_flags() {
        let cli = Cli::try_parse_from([
            "backtester",
            "-s",
            "grid",
            "-d",
            "data/test.csv",
            "-o",
            "csv",
            "-c",
            "config/test.toml",
        ])
        .unwrap();
        assert_eq!(cli.strategy.as_deref(), Some("grid"));
        assert_eq!(cli.data_file.as_deref(), Some("data/test.csv"));
        assert_eq!(cli.output, "csv");
        assert_eq!(cli.config.as_deref(), Some("config/test.toml"));
    }

    #[test]
    fn test_cli_binance_args() {
        let cli = Cli::try_parse_from([
            "backtester",
            "--source",
            "binance",
            "--binance-symbol",
            "ETHUSDT",
            "--interval",
            "5m",
        ])
        .unwrap();
        assert_eq!(cli.source.as_deref(), Some("binance"));
        assert_eq!(cli.binance_symbol.as_deref(), Some("ETHUSDT"));
        assert_eq!(cli.interval.as_deref(), Some("5m"));
    }

    // -----------------------------------------------------------------------
    // Config override tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_apply_cli_overrides_strategy() {
        let mut cfg = CliConfig::default();
        let cli = Cli::try_parse_from(["backtester", "--strategy", "grid"]).unwrap();
        apply_cli_overrides(&mut cfg, &cli);
        assert_eq!(cfg.strategy_type, "grid");
    }

    #[test]
    fn test_apply_cli_overrides_synthetic_clears_data_file() {
        let mut cfg = CliConfig::default();
        cfg.data_file = "some/path.jsonl".to_string();
        let cli = Cli::try_parse_from(["backtester", "--synthetic"]).unwrap();
        apply_cli_overrides(&mut cfg, &cli);
        assert_eq!(cfg.source, "synthetic");
        assert!(cfg.data_file.is_empty());
    }

    #[test]
    fn test_apply_cli_overrides_synthetic_with_data_file() {
        let mut cfg = CliConfig::default();
        let cli =
            Cli::try_parse_from(["backtester", "--synthetic", "--data-file", "override.jsonl"])
                .unwrap();
        apply_cli_overrides(&mut cfg, &cli);
        assert_eq!(cfg.source, "synthetic");
        assert_eq!(cfg.data_file, "override.jsonl");
    }

    #[test]
    fn test_apply_cli_overrides_initial_capital() {
        let mut cfg = CliConfig::default();
        let cli = Cli::try_parse_from(["backtester", "--initial-capital", "50000"]).unwrap();
        apply_cli_overrides(&mut cfg, &cli);
        assert_eq!(cfg.initial_capital, dec!(50000));
    }

    #[test]
    fn test_apply_cli_overrides_maker_fee_bps() {
        let mut cfg = CliConfig::default();
        let cli = Cli::try_parse_from(["backtester", "--maker-fee-bps", "10"]).unwrap();
        apply_cli_overrides(&mut cfg, &cli);
        assert_eq!(cfg.fee_rate, dec!(0.001)); // 10 / 10000 = 0.001
    }

    #[test]
    fn test_apply_cli_overrides_taker_fee_bps_overrides_maker() {
        let mut cfg = CliConfig::default();
        let cli = Cli::try_parse_from([
            "backtester",
            "--maker-fee-bps",
            "10",
            "--taker-fee-bps",
            "15",
        ])
        .unwrap();
        apply_cli_overrides(&mut cfg, &cli);
        // taker_fee_bps is applied after maker_fee_bps, so it wins
        assert_eq!(cfg.fee_rate, dec!(0.0015)); // 15 / 10000 = 0.0015
    }

    #[test]
    fn test_apply_cli_overrides_output_format() {
        let mut cfg = CliConfig::default();
        let cli = Cli::try_parse_from(["backtester", "--output", "json"]).unwrap();
        apply_cli_overrides(&mut cfg, &cli);
        assert_eq!(cfg.output_format, "json");
    }

    #[test]
    fn test_apply_cli_overrides_optimize() {
        let mut cfg = CliConfig::default();
        let cli = Cli::try_parse_from(["backtester", "--optimize"]).unwrap();
        apply_cli_overrides(&mut cfg, &cli);
        assert!(cfg.optimize);
    }

    #[test]
    fn test_apply_cli_overrides_preserves_defaults_when_no_args() {
        let mut cfg = CliConfig::default();
        let cli = Cli::try_parse_from(["backtester"]).unwrap();
        apply_cli_overrides(&mut cfg, &cli);
        // Defaults should be preserved
        assert_eq!(cfg.strategy_type, "spread");
        assert_eq!(cfg.initial_capital, dec!(100_000));
        assert_eq!(cfg.fee_rate, dec!(0.001));
        assert_eq!(cfg.spread_bps, dec!(10));
        assert_eq!(cfg.order_size, dec!(0.01));
        assert_eq!(cfg.num_levels, 3);
        assert_eq!(cfg.num_events, 1000);
        assert!(!cfg.optimize);
    }

    // -----------------------------------------------------------------------
    // Params key=value parsing tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_apply_params_basic() {
        let mut cfg = CliConfig::default();
        let params = vec![
            "spread_bps=15".to_string(),
            "order_size=0.1".to_string(),
            "num_levels=5".to_string(),
        ];
        apply_params(&mut cfg, &params);
        assert_eq!(cfg.spread_bps, dec!(15));
        assert_eq!(cfg.order_size, dec!(0.1));
        assert_eq!(cfg.num_levels, 5);
    }

    #[test]
    fn test_apply_params_all_known_keys() {
        let mut cfg = CliConfig::default();
        let params = vec![
            "spread_bps=20".to_string(),
            "order_size=0.5".to_string(),
            "num_levels=10".to_string(),
            "grid_spacing=100".to_string(),
            "gamma=0.5".to_string(),
            "sigma=0.03".to_string(),
            "fee_rate=0.002".to_string(),
            "slippage_bps=5".to_string(),
            "initial_capital=200000".to_string(),
            "num_events=2000".to_string(),
        ];
        apply_params(&mut cfg, &params);
        assert_eq!(cfg.spread_bps, dec!(20));
        assert_eq!(cfg.order_size, dec!(0.5));
        assert_eq!(cfg.num_levels, 10);
        assert_eq!(cfg.grid_spacing, dec!(100));
        assert_eq!(cfg.gamma, dec!(0.5));
        assert_eq!(cfg.sigma, dec!(0.03));
        assert_eq!(cfg.fee_rate, dec!(0.002));
        assert_eq!(cfg.slippage_bps, dec!(5));
        assert_eq!(cfg.initial_capital, dec!(200000));
        assert_eq!(cfg.num_events, 2000);
    }

    #[test]
    fn test_apply_params_ignores_unknown_keys() {
        let mut cfg = CliConfig::default();
        let original_spread = cfg.spread_bps;
        let params = vec!["unknown_key=42".to_string()];
        apply_params(&mut cfg, &params);
        // Should not change any known field
        assert_eq!(cfg.spread_bps, original_spread);
    }

    #[test]
    fn test_apply_params_ignores_invalid_format() {
        let mut cfg = CliConfig::default();
        let original_spread = cfg.spread_bps;
        let params = vec!["no_equals_sign".to_string()];
        apply_params(&mut cfg, &params);
        assert_eq!(cfg.spread_bps, original_spread);
    }

    #[test]
    fn test_apply_params_handles_whitespace() {
        let mut cfg = CliConfig::default();
        let params = vec!["  spread_bps = 25  ".to_string()];
        apply_params(&mut cfg, &params);
        assert_eq!(cfg.spread_bps, dec!(25));
    }

    #[test]
    fn test_apply_params_skips_empty() {
        let mut cfg = CliConfig::default();
        let original = cfg.spread_bps;
        let params = vec!["".to_string(), "  ".to_string()];
        apply_params(&mut cfg, &params);
        assert_eq!(cfg.spread_bps, original);
    }

    #[test]
    fn test_apply_params_invalid_decimal_ignored() {
        let mut cfg = CliConfig::default();
        let original = cfg.spread_bps;
        let params = vec!["spread_bps=not_a_number".to_string()];
        apply_params(&mut cfg, &params);
        // Invalid decimal should be silently ignored
        assert_eq!(cfg.spread_bps, original);
    }

    // -----------------------------------------------------------------------
    // Output format validation tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_output_format_text_is_valid() {
        let valid_formats = ["text", "json", "csv"];
        assert!(valid_formats.contains(&"text"));
    }

    #[test]
    fn test_output_format_json_is_valid() {
        let valid_formats = ["text", "json", "csv"];
        assert!(valid_formats.contains(&"json"));
    }

    #[test]
    fn test_output_format_csv_is_valid() {
        let valid_formats = ["text", "json", "csv"];
        assert!(valid_formats.contains(&"csv"));
    }

    #[test]
    fn test_output_format_invalid() {
        let valid_formats = ["text", "json", "csv"];
        assert!(!valid_formats.contains(&"xml"));
        assert!(!valid_formats.contains(&"yaml"));
    }

    // -----------------------------------------------------------------------
    // Integration: CLI -> Config flow
    // -----------------------------------------------------------------------

    #[test]
    fn test_full_cli_to_config_flow() {
        let mut cfg = CliConfig::default();
        let cli = Cli::try_parse_from([
            "backtester",
            "--strategy",
            "avellaneda",
            "--synthetic",
            "--initial-capital",
            "50000",
            "--fee-rate",
            "0.0005",
            "--spread-bps",
            "15",
            "--order-size",
            "0.05",
            "--num-levels",
            "5",
            "--gamma",
            "0.2",
            "--sigma",
            "0.03",
            "--output",
            "json",
            "--num-events",
            "500",
        ])
        .unwrap();
        apply_cli_overrides(&mut cfg, &cli);

        assert_eq!(cfg.strategy_type, "avellaneda");
        assert_eq!(cfg.source, "synthetic");
        assert_eq!(cfg.initial_capital, dec!(50000));
        assert_eq!(cfg.fee_rate, dec!(0.0005));
        assert_eq!(cfg.spread_bps, dec!(15));
        assert_eq!(cfg.order_size, dec!(0.05));
        assert_eq!(cfg.num_levels, 5);
        assert_eq!(cfg.gamma, dec!(0.2));
        assert_eq!(cfg.sigma, dec!(0.03));
        assert_eq!(cfg.output_format, "json");
        assert_eq!(cfg.num_events, 500);
    }

    #[test]
    fn test_params_override_after_cli_args() {
        let mut cfg = CliConfig::default();
        let cli = Cli::try_parse_from([
            "backtester",
            "--spread-bps",
            "20",
            "--params",
            "spread_bps=25",
        ])
        .unwrap();
        apply_cli_overrides(&mut cfg, &cli);
        // --params should override --spread-bps since it is applied last
        assert_eq!(cfg.spread_bps, dec!(25));
    }
}
