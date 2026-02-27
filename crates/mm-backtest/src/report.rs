use std::collections::BTreeMap;
use std::path::PathBuf;

use chrono::{DateTime, Duration, NaiveDate, Utc};
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use serde::{Deserialize, Serialize};

use mm_core::{Fill, Side};

// ---------------------------------------------------------------------------
// BacktestResult
// ---------------------------------------------------------------------------

/// Comprehensive result of a backtest run.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BacktestResult {
    /// Name of the strategy that was backtested.
    #[serde(default)]
    pub strategy_name: String,
    /// Gross PnL (before fees).
    pub total_pnl: Decimal,
    /// Total fees paid.
    pub total_fees: Decimal,
    /// Net PnL (total_pnl - total_fees).
    pub net_pnl: Decimal,
    /// Number of round-trip trades completed.
    pub num_trades: usize,
    /// Number of individual fills.
    pub num_fills: usize,
    /// Percentage of profitable round-trip trades (0.0 to 1.0).
    pub win_rate: Decimal,
    /// Maximum peak-to-trough drawdown in absolute terms.
    pub max_drawdown: Decimal,
    /// Maximum drawdown as a percentage of peak equity.
    pub max_drawdown_pct: Decimal,
    /// Annualized Sharpe ratio (using 365 days for crypto).
    pub sharpe_ratio: Decimal,
    /// Annualized Sortino ratio (using 365 days for crypto).
    pub sortino_ratio: Decimal,
    /// Average PnL per round-trip trade.
    pub avg_trade_pnl: Decimal,
    /// Profit factor: gross_profit / gross_loss. Infinity if no losses.
    pub profit_factor: Decimal,
    /// Total volume traded (sum of |price * quantity| across all fills).
    pub total_volume: Decimal,
    /// Backtest start time.
    pub start_time: DateTime<Utc>,
    /// Backtest end time.
    pub end_time: DateTime<Utc>,
    /// Duration of the backtest.
    pub duration: Duration,
    /// Equity at each fill event: (timestamp, equity).
    pub equity_curve: Vec<(DateTime<Utc>, Decimal)>,
}

// ---------------------------------------------------------------------------
// RoundTripTrade (internal tracking)
// ---------------------------------------------------------------------------

/// Tracks a complete round-trip trade (entry + exit).
#[derive(Debug, Clone)]
struct RoundTripTrade {
    pnl: Decimal,
}

// ---------------------------------------------------------------------------
// PerformanceCalculator
// ---------------------------------------------------------------------------

/// Accumulates fill data and computes performance statistics.
pub struct PerformanceCalculator {
    strategy_name: String,
    initial_capital: Decimal,
    /// Running equity (initial_capital + cumulative realized PnL - cumulative fees).
    equity: Decimal,
    /// Cumulative gross PnL (position PnL before fees).
    cumulative_pnl: Decimal,
    /// Cumulative fees.
    cumulative_fees: Decimal,
    /// Current net position quantity (positive = long, negative = short).
    net_position: Decimal,
    /// Weighted average entry price of the current position.
    avg_entry_price: Decimal,
    /// Completed round-trip trades.
    trades: Vec<RoundTripTrade>,
    /// All recorded fills.
    fill_count: usize,
    /// Total notional volume.
    total_volume: Decimal,
    /// Equity curve: (timestamp, equity).
    equity_curve: Vec<(DateTime<Utc>, Decimal)>,
    /// Daily equity snapshots for ratio calculations, keyed by date.
    daily_equity: BTreeMap<NaiveDate, Decimal>,
    /// Peak equity (for drawdown calculation).
    peak_equity: Decimal,
    /// Maximum drawdown seen so far.
    max_drawdown: Decimal,
    /// Maximum drawdown percentage seen so far.
    max_drawdown_pct: Decimal,
    /// Start time (first event).
    start_time: Option<DateTime<Utc>>,
    /// End time (last event).
    end_time: Option<DateTime<Utc>>,
}

impl PerformanceCalculator {
    pub fn new(initial_capital: Decimal) -> Self {
        Self {
            strategy_name: String::new(),
            initial_capital,
            equity: initial_capital,
            cumulative_pnl: Decimal::ZERO,
            cumulative_fees: Decimal::ZERO,
            net_position: Decimal::ZERO,
            avg_entry_price: Decimal::ZERO,
            trades: Vec::new(),
            fill_count: 0,
            total_volume: Decimal::ZERO,
            equity_curve: vec![],
            daily_equity: BTreeMap::new(),
            peak_equity: initial_capital,
            max_drawdown: Decimal::ZERO,
            max_drawdown_pct: Decimal::ZERO,
            start_time: None,
            end_time: None,
        }
    }

    /// Set the strategy name for the result.
    pub fn set_strategy_name(&mut self, name: impl Into<String>) {
        self.strategy_name = name.into();
    }

    /// Record a fill and update running statistics.
    pub fn record_fill(&mut self, fill: &Fill) {
        self.fill_count += 1;

        // Track time range
        if self.start_time.is_none() {
            self.start_time = Some(fill.timestamp);
        }
        self.end_time = Some(fill.timestamp);

        // Accumulate fees
        self.cumulative_fees += fill.fee;

        // Accumulate volume
        self.total_volume += fill.price * fill.quantity;

        // Determine signed quantity
        let signed_qty = match fill.side {
            Side::Buy => fill.quantity,
            Side::Sell => -fill.quantity,
        };

        let prev_position = self.net_position;
        let new_position = prev_position + signed_qty;

        // Check if this fill reduces the position (and thus realizes PnL)
        let is_reducing = (prev_position > Decimal::ZERO && signed_qty < Decimal::ZERO)
            || (prev_position < Decimal::ZERO && signed_qty > Decimal::ZERO);

        if is_reducing {
            // Quantity that actually reduces (not flips)
            let reducing_qty = fill.quantity.min(prev_position.abs());

            let pnl_per_unit = if prev_position > Decimal::ZERO {
                // Was long, selling to reduce -> PnL = fill_price - avg_entry
                fill.price - self.avg_entry_price
            } else {
                // Was short, buying to reduce -> PnL = avg_entry - fill_price
                self.avg_entry_price - fill.price
            };

            let trade_pnl = pnl_per_unit * reducing_qty;
            self.cumulative_pnl += trade_pnl;

            self.trades.push(RoundTripTrade { pnl: trade_pnl });

            // If the fill flips the position, the excess quantity starts a new position
            if new_position != Decimal::ZERO
                && ((prev_position > Decimal::ZERO && new_position < Decimal::ZERO)
                    || (prev_position < Decimal::ZERO && new_position > Decimal::ZERO))
            {
                // Flipped: new position starts at fill price
                self.avg_entry_price = fill.price;
            }
            // If the position is now flat or reduced but same side, avg_entry stays the same
        } else if prev_position == Decimal::ZERO {
            // Opening a new position
            self.avg_entry_price = fill.price;
        } else {
            // Adding to an existing position, update weighted average entry
            let total_cost =
                self.avg_entry_price * prev_position.abs() + fill.price * fill.quantity;
            let total_qty = prev_position.abs() + fill.quantity;
            if total_qty != Decimal::ZERO {
                self.avg_entry_price = total_cost / total_qty;
            }
        }

        self.net_position = new_position;

        // Update equity: initial_capital + cumulative_pnl - cumulative_fees
        self.equity = self.initial_capital + self.cumulative_pnl - self.cumulative_fees;

        // Record equity point
        self.equity_curve.push((fill.timestamp, self.equity));

        // Track daily equity snapshot (overwrite with latest for each day)
        let date = fill.timestamp.date_naive();
        self.daily_equity.insert(date, self.equity);

        // Update drawdown
        if self.equity > self.peak_equity {
            self.peak_equity = self.equity;
        }
        let drawdown = self.peak_equity - self.equity;
        if drawdown > self.max_drawdown {
            self.max_drawdown = drawdown;
        }
        if self.peak_equity > Decimal::ZERO {
            let dd_pct = drawdown / self.peak_equity * dec!(100);
            if dd_pct > self.max_drawdown_pct {
                self.max_drawdown_pct = dd_pct;
            }
        }
    }

    /// Compute all final statistics and return the backtest result.
    pub fn finalize(self) -> BacktestResult {
        let net_pnl = self.cumulative_pnl - self.cumulative_fees;

        let num_trades = self.trades.len();

        let winning_trades = self.trades.iter().filter(|t| t.pnl > Decimal::ZERO).count();
        let win_rate = if num_trades > 0 {
            Decimal::from(winning_trades as u64) / Decimal::from(num_trades as u64)
        } else {
            Decimal::ZERO
        };

        let avg_trade_pnl = if num_trades > 0 {
            self.trades.iter().map(|t| t.pnl).sum::<Decimal>() / Decimal::from(num_trades as u64)
        } else {
            Decimal::ZERO
        };

        let gross_profit: Decimal = self
            .trades
            .iter()
            .filter(|t| t.pnl > Decimal::ZERO)
            .map(|t| t.pnl)
            .sum();

        let gross_loss: Decimal = self
            .trades
            .iter()
            .filter(|t| t.pnl < Decimal::ZERO)
            .map(|t| t.pnl.abs())
            .sum();

        let profit_factor = if gross_loss > Decimal::ZERO {
            gross_profit / gross_loss
        } else if gross_profit > Decimal::ZERO {
            dec!(999999) // Represent "infinity" as a large number
        } else {
            Decimal::ZERO
        };

        // Compute daily returns for Sharpe/Sortino
        let daily_returns = self.compute_daily_returns();
        let sharpe_ratio = self.compute_sharpe(&daily_returns);
        let sortino_ratio = self.compute_sortino(&daily_returns);

        let start_time = self.start_time.unwrap_or_else(Utc::now);
        let end_time = self.end_time.unwrap_or_else(Utc::now);
        let duration = end_time - start_time;

        BacktestResult {
            strategy_name: self.strategy_name,
            total_pnl: self.cumulative_pnl,
            total_fees: self.cumulative_fees,
            net_pnl,
            num_trades,
            num_fills: self.fill_count,
            win_rate,
            max_drawdown: self.max_drawdown,
            max_drawdown_pct: self.max_drawdown_pct,
            sharpe_ratio,
            sortino_ratio,
            avg_trade_pnl,
            profit_factor,
            total_volume: self.total_volume,
            start_time,
            end_time,
            duration,
            equity_curve: self.equity_curve,
        }
    }

    // -----------------------------------------------------------------------
    // Daily Returns
    // -----------------------------------------------------------------------

    /// Compute daily percentage returns from the daily equity snapshots.
    fn compute_daily_returns(&self) -> Vec<f64> {
        let equities: Vec<Decimal> = self.daily_equity.values().cloned().collect();
        if equities.len() < 2 {
            return vec![];
        }

        let mut returns = Vec::with_capacity(equities.len() - 1);
        // Start from initial_capital for the first day's return
        let mut prev = self.initial_capital;
        for eq in &equities {
            if prev > Decimal::ZERO {
                let ret = (*eq - prev) / prev;
                returns.push(ret.to_f64().unwrap_or(0.0));
            }
            prev = *eq;
        }
        returns
    }

    // -----------------------------------------------------------------------
    // Sharpe Ratio
    // -----------------------------------------------------------------------

    /// Sharpe = mean(daily_returns) / std(daily_returns) * sqrt(365)
    fn compute_sharpe(&self, daily_returns: &[f64]) -> Decimal {
        if daily_returns.len() < 2 {
            return Decimal::ZERO;
        }

        let n = daily_returns.len() as f64;
        let mean = daily_returns.iter().sum::<f64>() / n;
        let variance = daily_returns
            .iter()
            .map(|r| (r - mean).powi(2))
            .sum::<f64>()
            / (n - 1.0);
        let std_dev = variance.sqrt();

        if std_dev < 1e-12 {
            return Decimal::ZERO;
        }

        let sharpe = (mean / std_dev) * (365.0_f64).sqrt();
        Decimal::from_f64_retain(sharpe).unwrap_or(Decimal::ZERO)
    }

    // -----------------------------------------------------------------------
    // Sortino Ratio
    // -----------------------------------------------------------------------

    /// Sortino = mean(daily_returns) / downside_std(daily_returns) * sqrt(365)
    fn compute_sortino(&self, daily_returns: &[f64]) -> Decimal {
        if daily_returns.len() < 2 {
            return Decimal::ZERO;
        }

        let n = daily_returns.len() as f64;
        let mean = daily_returns.iter().sum::<f64>() / n;

        let downside_variance = daily_returns
            .iter()
            .map(|r| if *r < 0.0 { r.powi(2) } else { 0.0 })
            .sum::<f64>()
            / (n - 1.0);

        let downside_std = downside_variance.sqrt();

        if downside_std < 1e-12 {
            return Decimal::ZERO;
        }

        let sortino = (mean / downside_std) * (365.0_f64).sqrt();
        Decimal::from_f64_retain(sortino).unwrap_or(Decimal::ZERO)
    }
}

// ---------------------------------------------------------------------------
// Report Printer
// ---------------------------------------------------------------------------

/// Print a formatted performance report to the console.
pub fn print_report(result: &BacktestResult) {
    println!();
    println!("============================================================");
    println!("                    BACKTEST REPORT                          ");
    println!("============================================================");
    println!();
    if !result.strategy_name.is_empty() {
        println!("  Strategy:         {}", result.strategy_name);
    }
    println!(
        "  Period:           {} to {}",
        result.start_time.format("%Y-%m-%d %H:%M"),
        result.end_time.format("%Y-%m-%d %H:%M")
    );
    println!("  Duration:         {} days", result.duration.num_days());
    println!();
    println!("  --- PnL ---");
    println!("  Gross PnL:        {}", format_decimal(result.total_pnl));
    println!("  Total Fees:       {}", format_decimal(result.total_fees));
    println!("  Net PnL:          {}", format_decimal(result.net_pnl));
    println!();
    println!("  --- Trades ---");
    println!("  Num Fills:        {}", result.num_fills);
    println!("  Num Trades:       {}", result.num_trades);
    println!(
        "  Win Rate:         {}%",
        format_decimal(result.win_rate * dec!(100))
    );
    println!(
        "  Avg Trade PnL:    {}",
        format_decimal(result.avg_trade_pnl)
    );
    println!(
        "  Profit Factor:    {}",
        format_decimal(result.profit_factor)
    );
    println!(
        "  Total Volume:     {}",
        format_decimal(result.total_volume)
    );
    println!();
    println!("  --- Risk ---");
    println!(
        "  Max Drawdown:     {}",
        format_decimal(result.max_drawdown)
    );
    println!(
        "  Max Drawdown %:   {}%",
        format_decimal(result.max_drawdown_pct)
    );
    println!("  Sharpe Ratio:     {}", format_ratio(result.sharpe_ratio));
    println!("  Sortino Ratio:    {}", format_ratio(result.sortino_ratio));
    println!();
    println!(
        "  Equity Curve:     {} data points",
        result.equity_curve.len()
    );
    println!("============================================================");
    println!();
}

/// Format a Decimal for display, rounding to 2 decimal places.
fn format_decimal(d: Decimal) -> String {
    format!("{:.2}", d)
}

/// Format a ratio (Sharpe/Sortino) to 4 decimal places.
fn format_ratio(d: Decimal) -> String {
    format!("{:.4}", d)
}

// ---------------------------------------------------------------------------
// BacktestResultSummary — lightweight summary for listings
// ---------------------------------------------------------------------------

/// Lightweight summary of a saved backtest result.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BacktestResultSummary {
    /// Unique identifier (timestamp + strategy name).
    pub id: String,
    /// Name of the strategy that was backtested.
    pub strategy_name: String,
    /// Net PnL (total_pnl - total_fees).
    pub net_pnl: Decimal,
    /// Annualized Sharpe ratio.
    pub sharpe_ratio: Decimal,
    /// Maximum drawdown as a percentage of peak equity.
    pub max_drawdown_pct: Decimal,
    /// Number of round-trip trades completed.
    pub num_trades: usize,
}

// ---------------------------------------------------------------------------
// BacktestComparison — compare two backtest runs
// ---------------------------------------------------------------------------

/// Side-by-side comparison of two backtest runs.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BacktestComparison {
    /// Summary of the first run.
    pub run_a: BacktestResultSummary,
    /// Summary of the second run.
    pub run_b: BacktestResultSummary,
    /// Difference in net PnL (b - a).
    pub pnl_diff: Decimal,
    /// Difference in Sharpe ratio (b - a).
    pub sharpe_diff: Decimal,
    /// Difference in max drawdown percentage (b - a).
    pub drawdown_diff: Decimal,
    /// Difference in trade count (b - a).
    pub trade_count_diff: i64,
    /// Which run is better based on Sharpe ratio: "a", "b", or "tie".
    pub better_run: String,
}

// ---------------------------------------------------------------------------
// BacktestResultStore — persistent file-based store
// ---------------------------------------------------------------------------

/// Persistent file-based store for backtest results.
///
/// Each result is saved as a pretty-printed JSON file in the `base_dir`.
/// File names are auto-generated from the timestamp and strategy name,
/// serving as unique identifiers.
pub struct BacktestResultStore {
    base_dir: PathBuf,
}

impl BacktestResultStore {
    /// Create a new store backed by the given directory.
    pub fn new(base_dir: impl Into<PathBuf>) -> Self {
        Self {
            base_dir: base_dir.into(),
        }
    }

    /// Save a result with an auto-generated ID based on timestamp + strategy.
    ///
    /// Returns the ID (filename stem) on success.
    pub fn save(&self, result: &BacktestResult) -> Result<String, String> {
        let strategy_part = if result.strategy_name.is_empty() {
            "unknown".to_string()
        } else {
            result.strategy_name.replace(['/', ' '], "_")
        };
        let now = chrono::Utc::now();
        let id = format!(
            "{}_{:06}_{}",
            now.format("%Y%m%d_%H%M%S"),
            now.timestamp_subsec_micros(),
            strategy_part
        );
        let path = self.base_dir.join(format!("{}.json", id));
        std::fs::create_dir_all(&self.base_dir).map_err(|e| e.to_string())?;
        let json = serde_json::to_string_pretty(result).map_err(|e| e.to_string())?;
        std::fs::write(&path, json).map_err(|e| e.to_string())?;
        Ok(id)
    }

    /// Load a result by ID (the filename stem without `.json`).
    pub fn load(&self, id: &str) -> Result<BacktestResult, String> {
        let path = self.base_dir.join(format!("{}.json", id));
        let json = std::fs::read_to_string(&path).map_err(|e| e.to_string())?;
        serde_json::from_str(&json).map_err(|e| e.to_string())
    }

    /// List all saved result IDs with basic metadata, newest first.
    pub fn list(&self) -> Result<Vec<BacktestResultSummary>, String> {
        let mut results = Vec::new();
        if !self.base_dir.exists() {
            return Ok(results);
        }
        for entry in std::fs::read_dir(&self.base_dir).map_err(|e| e.to_string())? {
            let entry = entry.map_err(|e| e.to_string())?;
            let path = entry.path();
            if path.extension().is_some_and(|ext| ext == "json") {
                let id = path
                    .file_stem()
                    .and_then(|s| s.to_str())
                    .unwrap_or("")
                    .to_string();
                if let Ok(result) = self.load(&id) {
                    results.push(BacktestResultSummary {
                        id,
                        strategy_name: result.strategy_name,
                        net_pnl: result.net_pnl,
                        sharpe_ratio: result.sharpe_ratio,
                        max_drawdown_pct: result.max_drawdown_pct,
                        num_trades: result.num_trades,
                    });
                }
            }
        }
        // Sort newest first (IDs start with timestamps).
        results.sort_by(|a, b| b.id.cmp(&a.id));
        Ok(results)
    }

    /// Compare two saved backtest results by their IDs.
    pub fn compare(&self, id_a: &str, id_b: &str) -> Result<BacktestComparison, String> {
        let a = self.load(id_a)?;
        let b = self.load(id_b)?;

        let summary_a = BacktestResultSummary {
            id: id_a.to_string(),
            strategy_name: a.strategy_name,
            net_pnl: a.net_pnl,
            sharpe_ratio: a.sharpe_ratio,
            max_drawdown_pct: a.max_drawdown_pct,
            num_trades: a.num_trades,
        };
        let summary_b = BacktestResultSummary {
            id: id_b.to_string(),
            strategy_name: b.strategy_name,
            net_pnl: b.net_pnl,
            sharpe_ratio: b.sharpe_ratio,
            max_drawdown_pct: b.max_drawdown_pct,
            num_trades: b.num_trades,
        };

        let pnl_diff = b.net_pnl - a.net_pnl;
        let sharpe_diff = b.sharpe_ratio - a.sharpe_ratio;
        let drawdown_diff = b.max_drawdown_pct - a.max_drawdown_pct;
        let trade_count_diff = b.num_trades as i64 - a.num_trades as i64;

        let better_run = if b.sharpe_ratio > a.sharpe_ratio {
            "b".to_string()
        } else if a.sharpe_ratio > b.sharpe_ratio {
            "a".to_string()
        } else {
            "tie".to_string()
        };

        Ok(BacktestComparison {
            run_a: summary_a,
            run_b: summary_b,
            pnl_diff,
            sharpe_diff,
            drawdown_diff,
            trade_count_diff,
            better_run,
        })
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use mm_core::{Exchange, Symbol};
    use rust_decimal_macros::dec;

    fn make_fill(
        side: Side,
        price: Decimal,
        quantity: Decimal,
        fee: Decimal,
        ts: DateTime<Utc>,
    ) -> Fill {
        Fill {
            fill_id: "f1".to_string(),
            client_order_id: "o1".to_string(),
            exchange_order_id: "e1".to_string(),
            exchange: Exchange::Simulated,
            symbol: Symbol::new("BTC", "USDT"),
            side,
            price,
            quantity,
            fee,
            fee_currency: "USDT".to_string(),
            timestamp: ts,
        }
    }

    #[test]
    fn test_round_trip_trade_pnl() {
        let mut calc = PerformanceCalculator::new(dec!(100_000));

        let t0 = Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap();
        let t1 = Utc.with_ymd_and_hms(2025, 1, 1, 1, 0, 0).unwrap();

        // Buy 1 BTC at 50000
        calc.record_fill(&make_fill(Side::Buy, dec!(50000), dec!(1), dec!(5), t0));
        // Sell 1 BTC at 51000
        calc.record_fill(&make_fill(Side::Sell, dec!(51000), dec!(1), dec!(5), t1));

        let result = calc.finalize();
        assert_eq!(result.total_pnl, dec!(1000)); // 51000 - 50000
        assert_eq!(result.total_fees, dec!(10)); // 5 + 5
        assert_eq!(result.net_pnl, dec!(990));
        assert_eq!(result.num_trades, 1);
        assert_eq!(result.num_fills, 2);
        assert_eq!(result.win_rate, dec!(1)); // 100%
    }

    #[test]
    fn test_losing_trade() {
        let mut calc = PerformanceCalculator::new(dec!(100_000));

        let t0 = Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap();
        let t1 = Utc.with_ymd_and_hms(2025, 1, 1, 1, 0, 0).unwrap();

        // Buy 1 BTC at 50000
        calc.record_fill(&make_fill(Side::Buy, dec!(50000), dec!(1), dec!(0), t0));
        // Sell 1 BTC at 49000
        calc.record_fill(&make_fill(Side::Sell, dec!(49000), dec!(1), dec!(0), t1));

        let result = calc.finalize();
        assert_eq!(result.total_pnl, dec!(-1000));
        assert_eq!(result.win_rate, Decimal::ZERO);
        assert!(result.max_drawdown > Decimal::ZERO);
    }

    #[test]
    fn test_multiple_trades_win_rate() {
        let mut calc = PerformanceCalculator::new(dec!(100_000));

        let t0 = Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap();
        let t1 = Utc.with_ymd_and_hms(2025, 1, 2, 0, 0, 0).unwrap();
        let t2 = Utc.with_ymd_and_hms(2025, 1, 3, 0, 0, 0).unwrap();
        let t3 = Utc.with_ymd_and_hms(2025, 1, 4, 0, 0, 0).unwrap();

        // Trade 1: Win (+500)
        calc.record_fill(&make_fill(Side::Buy, dec!(50000), dec!(1), dec!(0), t0));
        calc.record_fill(&make_fill(Side::Sell, dec!(50500), dec!(1), dec!(0), t1));

        // Trade 2: Loss (-300)
        calc.record_fill(&make_fill(Side::Buy, dec!(50000), dec!(1), dec!(0), t2));
        calc.record_fill(&make_fill(Side::Sell, dec!(49700), dec!(1), dec!(0), t3));

        let result = calc.finalize();
        assert_eq!(result.num_trades, 2);
        // 1 of 2 winning
        assert_eq!(result.win_rate, dec!(0.5));
    }

    #[test]
    fn test_profit_factor() {
        let mut calc = PerformanceCalculator::new(dec!(100_000));

        let t0 = Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap();
        let t1 = Utc.with_ymd_and_hms(2025, 1, 2, 0, 0, 0).unwrap();
        let t2 = Utc.with_ymd_and_hms(2025, 1, 3, 0, 0, 0).unwrap();
        let t3 = Utc.with_ymd_and_hms(2025, 1, 4, 0, 0, 0).unwrap();

        // Win +1000
        calc.record_fill(&make_fill(Side::Buy, dec!(50000), dec!(1), dec!(0), t0));
        calc.record_fill(&make_fill(Side::Sell, dec!(51000), dec!(1), dec!(0), t1));

        // Lose -500
        calc.record_fill(&make_fill(Side::Buy, dec!(50000), dec!(1), dec!(0), t2));
        calc.record_fill(&make_fill(Side::Sell, dec!(49500), dec!(1), dec!(0), t3));

        let result = calc.finalize();
        // profit_factor = 1000 / 500 = 2
        assert_eq!(result.profit_factor, dec!(2));
    }

    #[test]
    fn test_no_fills() {
        let calc = PerformanceCalculator::new(dec!(100_000));
        let result = calc.finalize();

        assert_eq!(result.total_pnl, Decimal::ZERO);
        assert_eq!(result.num_trades, 0);
        assert_eq!(result.num_fills, 0);
        assert_eq!(result.win_rate, Decimal::ZERO);
        assert_eq!(result.sharpe_ratio, Decimal::ZERO);
        assert_eq!(result.sortino_ratio, Decimal::ZERO);
    }

    #[test]
    fn test_equity_curve_tracking() {
        let mut calc = PerformanceCalculator::new(dec!(100_000));

        let t0 = Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap();
        let t1 = Utc.with_ymd_and_hms(2025, 1, 1, 1, 0, 0).unwrap();
        let t2 = Utc.with_ymd_and_hms(2025, 1, 1, 2, 0, 0).unwrap();
        let t3 = Utc.with_ymd_and_hms(2025, 1, 1, 3, 0, 0).unwrap();

        // Buy 1 BTC at 50000
        calc.record_fill(&make_fill(Side::Buy, dec!(50000), dec!(1), dec!(0), t0));
        // Sell 1 BTC at 51000 -> profit +1000, equity = 101000
        calc.record_fill(&make_fill(Side::Sell, dec!(51000), dec!(1), dec!(0), t1));
        // Buy 1 BTC at 51000
        calc.record_fill(&make_fill(Side::Buy, dec!(51000), dec!(1), dec!(0), t2));
        // Sell 1 BTC at 50500 -> loss -500, equity = 100500
        calc.record_fill(&make_fill(Side::Sell, dec!(50500), dec!(1), dec!(0), t3));

        let result = calc.finalize();
        assert_eq!(result.equity_curve.len(), 4);
        // After buy: no realized PnL yet, equity = 100000
        assert_eq!(result.equity_curve[0].1, dec!(100_000));
        // After sell at 51000: realized +1000, equity = 101000
        assert_eq!(result.equity_curve[1].1, dec!(101_000));
        // After buy: no new realized PnL, equity still 101000
        assert_eq!(result.equity_curve[2].1, dec!(101_000));
        // After sell at 50500: realized -500, equity = 100500
        assert_eq!(result.equity_curve[3].1, dec!(100_500));
    }

    #[test]
    fn test_max_drawdown_calculation() {
        let mut calc = PerformanceCalculator::new(dec!(100_000));

        let t0 = Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap();
        let t1 = Utc.with_ymd_and_hms(2025, 1, 1, 1, 0, 0).unwrap();
        let t2 = Utc.with_ymd_and_hms(2025, 1, 1, 2, 0, 0).unwrap();
        let t3 = Utc.with_ymd_and_hms(2025, 1, 1, 3, 0, 0).unwrap();

        // Win trade: buy 50000, sell 51000 -> equity = 101000
        calc.record_fill(&make_fill(Side::Buy, dec!(50000), dec!(1), dec!(0), t0));
        calc.record_fill(&make_fill(Side::Sell, dec!(51000), dec!(1), dec!(0), t1));

        // Losing trade: buy 50000, sell 48000 -> equity = 99000
        calc.record_fill(&make_fill(Side::Buy, dec!(50000), dec!(1), dec!(0), t2));
        calc.record_fill(&make_fill(Side::Sell, dec!(48000), dec!(1), dec!(0), t3));

        let result = calc.finalize();
        // Max drawdown = peak (101000) - trough (99000) = 2000
        assert_eq!(result.max_drawdown, dec!(2000));
        // Max drawdown % = 2000 / 101000 * 100 ≈ 1.98%
        let expected_dd_pct = dec!(2000) / dec!(101000) * dec!(100);
        assert_eq!(result.max_drawdown_pct, expected_dd_pct);
    }

    #[test]
    fn test_short_selling_pnl() {
        let mut calc = PerformanceCalculator::new(dec!(100_000));

        let t0 = Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap();
        let t1 = Utc.with_ymd_and_hms(2025, 1, 1, 1, 0, 0).unwrap();

        // Sell 1 BTC at 50000 (open short)
        calc.record_fill(&make_fill(Side::Sell, dec!(50000), dec!(1), dec!(0), t0));
        // Buy 1 BTC at 49000 (close short)
        calc.record_fill(&make_fill(Side::Buy, dec!(49000), dec!(1), dec!(0), t1));

        let result = calc.finalize();
        // Short PnL = entry (50000) - exit (49000) = +1000
        assert_eq!(result.total_pnl, dec!(1000));
    }

    #[test]
    fn test_partial_position_closing() {
        let mut calc = PerformanceCalculator::new(dec!(100_000));

        let t0 = Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap();
        let t1 = Utc.with_ymd_and_hms(2025, 1, 1, 1, 0, 0).unwrap();
        let t2 = Utc.with_ymd_and_hms(2025, 1, 1, 2, 0, 0).unwrap();

        // Buy 2.0 at 50000
        calc.record_fill(&make_fill(Side::Buy, dec!(50000), dec!(2), dec!(0), t0));
        // Sell 1.0 at 51000 -> realized PnL on 1.0 = +1000
        calc.record_fill(&make_fill(Side::Sell, dec!(51000), dec!(1), dec!(0), t1));
        // Sell 1.0 at 52000 -> realized PnL on remaining 1.0 = +2000
        calc.record_fill(&make_fill(Side::Sell, dec!(52000), dec!(1), dec!(0), t2));

        let result = calc.finalize();
        assert_eq!(result.total_pnl, dec!(3000));
        assert_eq!(result.num_trades, 2);
    }

    #[test]
    fn test_position_flip() {
        let mut calc = PerformanceCalculator::new(dec!(100_000));

        let t0 = Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap();
        let t1 = Utc.with_ymd_and_hms(2025, 1, 1, 1, 0, 0).unwrap();
        let t2 = Utc.with_ymd_and_hms(2025, 1, 1, 2, 0, 0).unwrap();

        // Buy 1.0 at 50000 (long 1.0)
        calc.record_fill(&make_fill(Side::Buy, dec!(50000), dec!(1), dec!(0), t0));
        // Sell 2.0 at 51000 (close long at +1000, open short 1.0)
        calc.record_fill(&make_fill(Side::Sell, dec!(51000), dec!(2), dec!(0), t1));
        // Buy 1.0 at 50500 (close short at +500)
        calc.record_fill(&make_fill(Side::Buy, dec!(50500), dec!(1), dec!(0), t2));

        let result = calc.finalize();
        assert_eq!(result.total_pnl, dec!(1500));
        assert_eq!(result.num_trades, 2);
    }

    #[test]
    fn test_adding_to_position_updates_avg_entry() {
        let mut calc = PerformanceCalculator::new(dec!(100_000));

        let t0 = Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap();
        let t1 = Utc.with_ymd_and_hms(2025, 1, 1, 1, 0, 0).unwrap();
        let t2 = Utc.with_ymd_and_hms(2025, 1, 1, 2, 0, 0).unwrap();

        // Buy 1.0 at 50000
        calc.record_fill(&make_fill(Side::Buy, dec!(50000), dec!(1), dec!(0), t0));
        // Buy 1.0 at 52000 -> avg entry = (50000 + 52000) / 2 = 51000
        calc.record_fill(&make_fill(Side::Buy, dec!(52000), dec!(1), dec!(0), t1));
        // Sell 2.0 at 53000 -> PnL = 2 * (53000 - 51000) = 4000
        calc.record_fill(&make_fill(Side::Sell, dec!(53000), dec!(2), dec!(0), t2));

        let result = calc.finalize();
        assert_eq!(result.total_pnl, dec!(4000));
    }

    // -----------------------------------------------------------------------
    // BacktestResultStore & Comparison tests
    // -----------------------------------------------------------------------

    /// Helper to build a minimal BacktestResult for store/comparison tests.
    fn make_result(
        strategy_name: &str,
        net_pnl: Decimal,
        sharpe: Decimal,
        num_trades: usize,
    ) -> BacktestResult {
        let now = Utc::now();
        BacktestResult {
            strategy_name: strategy_name.to_string(),
            total_pnl: net_pnl + dec!(10),
            total_fees: dec!(10),
            net_pnl,
            num_trades,
            num_fills: num_trades * 2,
            win_rate: dec!(0.5),
            max_drawdown: dec!(100),
            max_drawdown_pct: dec!(1),
            sharpe_ratio: sharpe,
            sortino_ratio: dec!(0),
            avg_trade_pnl: if num_trades > 0 {
                net_pnl / Decimal::from(num_trades as u64)
            } else {
                Decimal::ZERO
            },
            profit_factor: dec!(2),
            total_volume: dec!(50000),
            start_time: now,
            end_time: now + Duration::hours(1),
            duration: Duration::hours(1),
            equity_curve: vec![],
        }
    }

    #[test]
    fn test_result_store_save_and_load() {
        let dir = tempfile::tempdir().unwrap();
        let store = BacktestResultStore::new(dir.path());

        let result = make_result("spread", dec!(500), dec!(1.5), 10);
        let id = store.save(&result).expect("save should succeed");

        let loaded = store.load(&id).expect("load should succeed");
        assert_eq!(loaded.strategy_name, "spread");
        assert_eq!(loaded.net_pnl, dec!(500));
        assert_eq!(loaded.sharpe_ratio, dec!(1.5));
        assert_eq!(loaded.num_trades, 10);
    }

    #[test]
    fn test_result_store_list() {
        let dir = tempfile::tempdir().unwrap();
        let store = BacktestResultStore::new(dir.path());

        store
            .save(&make_result("spread", dec!(100), dec!(1.0), 5))
            .unwrap();
        // Sleep briefly so the IDs differ by timestamp
        std::thread::sleep(std::time::Duration::from_millis(1100));
        store
            .save(&make_result("grid", dec!(200), dec!(2.0), 8))
            .unwrap();

        let list = store.list().expect("list should succeed");
        assert_eq!(list.len(), 2);
        // Newest first — grid was saved last
        assert_eq!(list[0].strategy_name, "grid");
        assert_eq!(list[1].strategy_name, "spread");
    }

    #[test]
    fn test_result_store_list_empty_dir() {
        let dir = tempfile::tempdir().unwrap();
        let store = BacktestResultStore::new(dir.path());

        let list = store.list().expect("list should succeed");
        assert!(list.is_empty());
    }

    #[test]
    fn test_result_store_list_nonexistent_dir() {
        let store = BacktestResultStore::new("/tmp/nonexistent_backtest_dir_12345");
        let list = store.list().expect("list should succeed for missing dir");
        assert!(list.is_empty());
    }

    #[test]
    fn test_comparison_pnl_diff() {
        let dir = tempfile::tempdir().unwrap();
        let store = BacktestResultStore::new(dir.path());

        let id_a = store
            .save(&make_result("spread_a", dec!(100), dec!(1.0), 5))
            .unwrap();
        std::thread::sleep(std::time::Duration::from_millis(2));
        let id_b = store
            .save(&make_result("spread_b", dec!(300), dec!(1.5), 8))
            .unwrap();

        let cmp = store.compare(&id_a, &id_b).expect("compare should succeed");
        assert_eq!(cmp.pnl_diff, dec!(200)); // 300 - 100
        assert_eq!(cmp.trade_count_diff, 3); // 8 - 5
    }

    #[test]
    fn test_comparison_better_run() {
        let dir = tempfile::tempdir().unwrap();
        let store = BacktestResultStore::new(dir.path());

        let id_a = store
            .save(&make_result("spread", dec!(100), dec!(2.0), 5))
            .unwrap();
        let id_b = store
            .save(&make_result("grid", dec!(300), dec!(1.0), 8))
            .unwrap();

        let cmp = store.compare(&id_a, &id_b).expect("compare should succeed");
        // Run a has Sharpe 2.0 > 1.0, so a is better
        assert_eq!(cmp.better_run, "a");
        assert_eq!(cmp.sharpe_diff, dec!(-1.0)); // 1.0 - 2.0 = -1.0
    }

    #[test]
    fn test_comparison_tie() {
        let dir = tempfile::tempdir().unwrap();
        let store = BacktestResultStore::new(dir.path());

        let id_a = store
            .save(&make_result("spread", dec!(100), dec!(1.5), 5))
            .unwrap();
        let id_b = store
            .save(&make_result("grid", dec!(200), dec!(1.5), 8))
            .unwrap();

        let cmp = store.compare(&id_a, &id_b).expect("compare should succeed");
        assert_eq!(cmp.better_run, "tie");
    }

    #[test]
    fn test_result_summary_fields() {
        let dir = tempfile::tempdir().unwrap();
        let store = BacktestResultStore::new(dir.path());

        let id = store
            .save(&make_result("avellaneda", dec!(750), dec!(2.5), 12))
            .unwrap();

        let list = store.list().unwrap();
        assert_eq!(list.len(), 1);

        let summary = &list[0];
        assert_eq!(summary.id, id);
        assert_eq!(summary.strategy_name, "avellaneda");
        assert_eq!(summary.net_pnl, dec!(750));
        assert_eq!(summary.sharpe_ratio, dec!(2.5));
        assert_eq!(summary.max_drawdown_pct, dec!(1));
        assert_eq!(summary.num_trades, 12);
    }
}
