//! Backtesting CLI binary.
//!
//! Loads historical market events from a JSON lines file (or generates
//! synthetic sample data), creates a strategy, runs the replay engine,
//! and prints the performance report.

use std::io::BufRead;
use std::path::PathBuf;

use anyhow::{Context, Result};
use chrono::{Duration, Utc};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use serde::Deserialize;
use tracing::info;

use mm_backtest::{BacktestConfig, MarketEvent, ReplayEngine, print_report};
use mm_core::traits::Strategy;
use mm_core::{Exchange, PriceLevel, Side, Symbol};
use mm_strategy::{
    AvellanedaStoikovConfig, AvellanedaStoikovStrategy, GridConfig, GridStrategy, GridType,
    MeanReversionConfig, MeanReversionStrategy, MomentumConfig, MomentumStrategy, SpreadConfig,
    SpreadStrategy, StatArbConfig, StatArbStrategy, TwapConfig, TwapStrategy, VwapConfig,
    VwapStrategy,
};

// ---------------------------------------------------------------------------
// Configuration
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

    // Load configuration.
    let cfg = load_config().context("failed to load configuration")?;
    info!(
        strategy = %cfg.strategy_type,
        initial_capital = %cfg.initial_capital,
        fee_rate = %cfg.fee_rate,
        slippage_bps = %cfg.slippage_bps,
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
    // 2. Create strategy
    // ------------------------------------------------------------------
    let mut strategy: Box<dyn Strategy> = create_strategy(&cfg)?;
    info!(
        strategy_type = %cfg.strategy_type,
        strategy_name = strategy.name(),
        "strategy created"
    );

    // ------------------------------------------------------------------
    // 3. Configure and run the replay engine
    // ------------------------------------------------------------------
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

    // ------------------------------------------------------------------
    // 4. Print the report
    // ------------------------------------------------------------------
    print_report(&result);

    // Also emit structured log for automation.
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
            let trade_side = if seed % 2 == 0 { Side::Buy } else { Side::Sell };
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

fn load_config() -> Result<CliConfig> {
    let settings = config::Config::builder()
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
        // Config files (optional)
        .add_source(
            config::File::with_name("config/backtester")
                .required(false),
        )
        .add_source(
            config::File::with_name("config/default")
                .required(false),
        )
        // Environment variables (prefixed with BACKTEST_)
        .add_source(
            config::Environment::with_prefix("BACKTEST")
                .separator("_"),
        )
        .build()
        .context("failed to build config")?;

    settings
        .try_deserialize::<CliConfig>()
        .context("failed to deserialize config")
}
