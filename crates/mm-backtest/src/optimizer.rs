//! Parameter optimization via grid search over backtest runs.
//!
//! The [`ParameterOptimizer`] generates every combination of the supplied
//! [`ParameterRange`] values (cartesian product), runs a backtest for each one
//! using the [`ReplayEngine`], and returns the top-N results ranked by the
//! chosen [`OptimizationMetric`].

use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;
use serde::{Deserialize, Serialize};
use tracing::{info, warn};

use mm_core::traits::Strategy;
use mm_core::types::{Exchange, Side, Symbol};

use mm_strategy::{
    AvellanedaStoikovConfig, AvellanedaStoikovStrategy, GridConfig, GridStrategy, GridType,
    MeanReversionConfig, MeanReversionStrategy, MomentumConfig, MomentumStrategy, SpreadConfig,
    SpreadStrategy, StatArbConfig, StatArbStrategy, TwapConfig, TwapStrategy, VwapConfig,
    VwapStrategy,
};

use crate::replay::{BacktestConfig, MarketEvent, ReplayEngine};
use crate::report::BacktestResult;

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/// A named parameter with a list of candidate values to sweep over.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParameterRange {
    pub name: String,
    pub values: Vec<serde_json::Value>,
}

/// The metric used to rank optimisation results.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum OptimizationMetric {
    Sharpe,
    Sortino,
    TotalPnl,
    MaxDrawdown,
    ProfitFactor,
    WinRate,
}

/// Full configuration for a parameter-sweep optimisation run.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OptimizationConfig {
    /// Which strategy to instantiate (e.g. "spread", "grid", "avellaneda").
    pub strategy_type: String,
    /// Base strategy parameters (JSON object). Overridden per-combination.
    pub base_params: serde_json::Value,
    /// Parameter ranges to sweep.
    pub param_ranges: Vec<ParameterRange>,
    /// Backtest engine configuration.
    pub backtest_config: BacktestConfig,
    /// Metric to optimise for.
    pub metric: OptimizationMetric,
    /// Number of top results to return.
    pub top_n: usize,
}

/// A single optimisation result: the parameter set, its backtest result, and
/// the scalar metric value used for ranking.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OptimizationResult {
    pub params: serde_json::Value,
    pub backtest_result: BacktestResult,
    pub metric_value: f64,
}

/// Progress information emitted during a sweep.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OptimizationProgress {
    pub completed: usize,
    pub total: usize,
    pub best_so_far: Option<OptimizationResult>,
}

// ---------------------------------------------------------------------------
// Optimizer
// ---------------------------------------------------------------------------

/// Stateless grid-search optimiser.
pub struct ParameterOptimizer;

impl ParameterOptimizer {
    /// Run a grid search over all parameter combinations.
    ///
    /// For each combination the optimizer:
    /// 1. Merges override values into `base_params`.
    /// 2. Creates a strategy via the built-in factory.
    /// 3. Runs the [`ReplayEngine`] with the given events.
    /// 4. Extracts the requested metric and records the result.
    ///
    /// Results are sorted by metric (descending for most metrics, ascending
    /// for `MaxDrawdown`) and truncated to `top_n`.
    pub fn run(
        config: &OptimizationConfig,
        events: Vec<MarketEvent>,
        progress_tx: Option<tokio::sync::mpsc::Sender<OptimizationProgress>>,
    ) -> Vec<OptimizationResult> {
        let combinations = cartesian_product(&config.param_ranges);
        let total = combinations.len();

        info!(
            total_combinations = total,
            strategy_type = %config.strategy_type,
            metric = ?config.metric,
            "Starting parameter optimisation"
        );

        let mut results: Vec<OptimizationResult> = Vec::with_capacity(total);

        for (i, combo) in combinations.iter().enumerate() {
            // Merge this combination into the base params.
            let merged = merge_params(&config.base_params, combo);

            // Create strategy.
            let strategy = match create_strategy(&config.strategy_type, &merged) {
                Some(s) => s,
                None => {
                    warn!(
                        combination = i,
                        params = %merged,
                        "Skipping combination: failed to create strategy"
                    );
                    continue;
                }
            };

            // Run backtest.
            let engine = ReplayEngine::new(config.backtest_config.clone());
            let result = engine.run(&mut *strategy.into_inner(), events.clone());

            // Extract metric.
            let metric_value = extract_metric(&result, &config.metric);

            results.push(OptimizationResult {
                params: merged,
                backtest_result: result,
                metric_value,
            });

            // Report progress.
            if let Some(ref tx) = progress_tx {
                let best = results
                    .iter()
                    .max_by(|a, b| {
                        a.metric_value
                            .partial_cmp(&b.metric_value)
                            .unwrap_or(std::cmp::Ordering::Equal)
                    })
                    .cloned();

                let _ = tx.try_send(OptimizationProgress {
                    completed: i + 1,
                    total,
                    best_so_far: best,
                });
            }
        }

        // Sort: descending for most metrics, ascending for MaxDrawdown.
        results.sort_by(|a, b| {
            if config.metric == OptimizationMetric::MaxDrawdown {
                a.metric_value
                    .partial_cmp(&b.metric_value)
                    .unwrap_or(std::cmp::Ordering::Equal)
            } else {
                b.metric_value
                    .partial_cmp(&a.metric_value)
                    .unwrap_or(std::cmp::Ordering::Equal)
            }
        });

        results.truncate(config.top_n);

        info!(
            returned = results.len(),
            "Optimisation complete"
        );

        results
    }
}

// ---------------------------------------------------------------------------
// Helper: cartesian product
// ---------------------------------------------------------------------------

/// Generate the cartesian product of all parameter ranges.
///
/// Each element of the returned vector is a single combination expressed as a
/// `Vec<(name, value)>` list.
pub fn cartesian_product(
    ranges: &[ParameterRange],
) -> Vec<Vec<(String, serde_json::Value)>> {
    let mut result: Vec<Vec<(String, serde_json::Value)>> = vec![vec![]];

    for range in ranges {
        let mut next = Vec::new();
        for existing in &result {
            for value in &range.values {
                let mut combo = existing.clone();
                combo.push((range.name.clone(), value.clone()));
                next.push(combo);
            }
        }
        result = next;
    }

    result
}

// ---------------------------------------------------------------------------
// Helper: merge params
// ---------------------------------------------------------------------------

/// Merge override key-value pairs into a base JSON object.
///
/// Creates a shallow copy of `base` and sets each override key on it.
pub fn merge_params(
    base: &serde_json::Value,
    overrides: &[(String, serde_json::Value)],
) -> serde_json::Value {
    let mut merged = base.clone();

    if let Some(obj) = merged.as_object_mut() {
        for (key, value) in overrides {
            obj.insert(key.clone(), value.clone());
        }
    }

    merged
}

// ---------------------------------------------------------------------------
// Helper: extract metric
// ---------------------------------------------------------------------------

/// Extract a scalar f64 from a [`BacktestResult`] for the given metric.
pub fn extract_metric(result: &BacktestResult, metric: &OptimizationMetric) -> f64 {
    match metric {
        OptimizationMetric::Sharpe => {
            result.sharpe_ratio.to_f64().unwrap_or(0.0)
        }
        OptimizationMetric::Sortino => {
            result.sortino_ratio.to_f64().unwrap_or(0.0)
        }
        OptimizationMetric::TotalPnl => {
            result.total_pnl.to_f64().unwrap_or(0.0)
        }
        OptimizationMetric::MaxDrawdown => {
            result.max_drawdown.to_f64().unwrap_or(0.0)
        }
        OptimizationMetric::ProfitFactor => {
            result.profit_factor.to_f64().unwrap_or(0.0)
        }
        OptimizationMetric::WinRate => {
            result.win_rate.to_f64().unwrap_or(0.0)
        }
    }
}

// ---------------------------------------------------------------------------
// Helper: strategy factory
// ---------------------------------------------------------------------------

/// Wrapper around `Box<dyn Strategy>` that can be unwrapped. We need this
/// because `Box<dyn Strategy>` cannot be dereferenced to `&mut dyn Strategy`
/// in all generic contexts without a helper.
pub struct StrategyBox(Box<dyn Strategy>);

impl StrategyBox {
    /// Consume the wrapper and return a mutable reference-friendly box.
    pub fn into_inner(self) -> Box<dyn Strategy> {
        self.0
    }
}

/// Create a strategy from a type name and a JSON parameter object.
///
/// Returns `None` if the strategy type is unknown or deserialization fails.
pub fn create_strategy(
    strategy_type: &str,
    params: &serde_json::Value,
) -> Option<StrategyBox> {
    let exchange = params
        .get("exchange")
        .and_then(|v| serde_json::from_value::<Exchange>(v.clone()).ok())
        .unwrap_or(Exchange::Simulated);

    let symbol = params
        .get("symbol")
        .and_then(|v| serde_json::from_value::<Symbol>(v.clone()).ok())
        .unwrap_or_else(|| Symbol::new("BTC", "USDT"));

    let strategy_id = params
        .get("strategy_id")
        .and_then(|v| v.as_str())
        .unwrap_or("optimizer")
        .to_string();

    // Helper closures for reading decimals and integers from JSON.
    let dec_param = |key: &str, default: Decimal| -> Decimal {
        params
            .get(key)
            .and_then(|v| {
                if let Some(s) = v.as_str() {
                    s.parse().ok()
                } else if let Some(n) = v.as_f64() {
                    Decimal::from_f64_retain(n)
                } else if let Some(n) = v.as_i64() {
                    Some(Decimal::from(n))
                } else {
                    None
                }
            })
            .unwrap_or(default)
    };

    let usize_param = |key: &str, default: usize| -> usize {
        params
            .get(key)
            .and_then(|v| {
                v.as_u64().map(|n| n as usize).or_else(|| {
                    v.as_str().and_then(|s| s.parse().ok())
                })
            })
            .unwrap_or(default)
    };

    let i64_param = |key: &str, default: i64| -> i64 {
        params
            .get(key)
            .and_then(|v| {
                v.as_i64().or_else(|| v.as_str().and_then(|s| s.parse().ok()))
            })
            .unwrap_or(default)
    };

    let u64_param = |key: &str, default: u64| -> u64 {
        params
            .get(key)
            .and_then(|v| {
                v.as_u64().or_else(|| v.as_str().and_then(|s| s.parse().ok()))
            })
            .unwrap_or(default)
    };

    match strategy_type.to_lowercase().as_str() {
        "spread" => {
            let spread_bps = dec_param("spread_bps", Decimal::new(10, 0));
            let config = SpreadConfig {
                strategy_id,
                exchange,
                symbol,
                spread_bps,
                order_size: dec_param("order_size", Decimal::new(1, 2)), // 0.01
                num_levels: usize_param("num_levels", 3),
                refresh_interval_ms: 0,
                min_spread_bps: dec_param("min_spread_bps", spread_bps / Decimal::new(2, 0)),
                reprice_threshold_bps: dec_param("reprice_threshold_bps", spread_bps),
            };
            Some(StrategyBox(Box::new(SpreadStrategy::new(config))))
        }
        "grid" => {
            let grid_spacing = dec_param("grid_spacing", Decimal::new(50, 0));
            let num_grids = usize_param("num_grids", 5);
            let center = dec_param("center_price", Decimal::new(50000, 0));
            let half_range = grid_spacing * Decimal::from(num_grids as u64);
            let config = GridConfig {
                strategy_id,
                exchange,
                symbol,
                grid_type: GridType::Arithmetic,
                num_grids,
                order_size: dec_param("order_size", Decimal::new(1, 2)),
                upper_price: dec_param("upper_price", center + half_range),
                lower_price: dec_param("lower_price", center - half_range),
            };
            Some(StrategyBox(Box::new(GridStrategy::new(config))))
        }
        "avellaneda" => {
            let order_size = dec_param("order_size", Decimal::new(1, 2));
            let num_levels = usize_param("num_levels", 3);
            let config = AvellanedaStoikovConfig {
                strategy_id,
                exchange,
                symbol,
                gamma: dec_param("gamma", Decimal::new(1, 1)), // 0.1
                sigma: dec_param("sigma", Decimal::new(2, 2)), // 0.02
                kappa: dec_param("kappa", Decimal::new(15, 1)), // 1.5
                order_size,
                max_position: dec_param(
                    "max_position",
                    order_size * Decimal::from(num_levels as u64 * 2),
                ),
                session_duration_secs: i64_param("session_duration_secs", 3600),
            };
            Some(StrategyBox(Box::new(AvellanedaStoikovStrategy::new(config))))
        }
        "twap" => {
            let order_size = dec_param("order_size", Decimal::new(1, 2));
            let num_levels = usize_param("num_levels", 3);
            let config = TwapConfig {
                strategy_id,
                exchange,
                symbol,
                target_quantity: dec_param(
                    "target_quantity",
                    order_size * Decimal::from(num_levels as u64 * 10),
                ),
                duration_secs: u64_param("duration_secs", 3600),
                slice_interval_secs: u64_param("slice_interval_secs", 60),
                side: Side::Buy,
                max_deviation_bps: dec_param("max_deviation_bps", Decimal::new(10, 0)),
            };
            Some(StrategyBox(Box::new(TwapStrategy::new(config))))
        }
        "vwap" => {
            let order_size = dec_param("order_size", Decimal::new(1, 2));
            let num_levels = usize_param("num_levels", 3);
            let config = VwapConfig {
                strategy_id,
                exchange,
                symbol,
                target_quantity: dec_param(
                    "target_quantity",
                    order_size * Decimal::from(num_levels as u64 * 10),
                ),
                duration_secs: u64_param("duration_secs", 3600),
                side: Side::Buy,
                volume_profile: vec![
                    Decimal::new(1, 1),
                    Decimal::new(2, 1),
                    Decimal::new(3, 1),
                    Decimal::new(25, 2),
                    Decimal::new(15, 2),
                ],
                max_deviation_bps: dec_param("max_deviation_bps", Decimal::new(10, 0)),
            };
            Some(StrategyBox(Box::new(VwapStrategy::new(config))))
        }
        "momentum" => {
            let order_size = dec_param("order_size", Decimal::new(1, 2));
            let num_levels = usize_param("num_levels", 3);
            let spread_bps = dec_param("spread_bps", Decimal::new(10, 0));
            let config = MomentumConfig {
                strategy_id,
                exchange,
                symbol,
                lookback_periods: usize_param("lookback_periods", num_levels.max(2)),
                entry_threshold_bps: dec_param("entry_threshold_bps", spread_bps * Decimal::new(5, 0)),
                exit_threshold_bps: dec_param("exit_threshold_bps", spread_bps),
                order_size,
                trailing_stop_bps: dec_param("trailing_stop_bps", spread_bps * Decimal::new(3, 0)),
                max_position: dec_param(
                    "max_position",
                    order_size * Decimal::from(num_levels as u64 * 2),
                ),
            };
            Some(StrategyBox(Box::new(MomentumStrategy::new(config))))
        }
        "mean_reversion" => {
            let order_size = dec_param("order_size", Decimal::new(1, 2));
            let num_levels = usize_param("num_levels", 3);
            let config = MeanReversionConfig {
                strategy_id,
                exchange,
                symbol,
                window_periods: usize_param("window_periods", num_levels.max(2)),
                num_std_dev: dec_param("num_std_dev", Decimal::new(2, 0)),
                order_size,
                max_position: dec_param(
                    "max_position",
                    order_size * Decimal::from(num_levels as u64 * 2),
                ),
                exit_at_mean: true,
            };
            Some(StrategyBox(Box::new(MeanReversionStrategy::new(config))))
        }
        "stat_arb" => {
            let order_size = dec_param("order_size", Decimal::new(1, 2));
            let num_levels = usize_param("num_levels", 3);
            let config = StatArbConfig {
                strategy_id,
                leg_a_exchange: exchange,
                leg_a_symbol: symbol.clone(),
                leg_b_exchange: exchange,
                leg_b_symbol: symbol,
                lookback_periods: usize_param("lookback_periods", num_levels.max(2)),
                entry_z_score: dec_param("entry_z_score", Decimal::new(2, 0)),
                exit_z_score: dec_param("exit_z_score", Decimal::new(5, 1)), // 0.5
                order_size,
                max_position: dec_param(
                    "max_position",
                    order_size * Decimal::from(num_levels as u64 * 2),
                ),
            };
            Some(StrategyBox(Box::new(StatArbStrategy::new(config))))
        }
        other => {
            warn!(strategy_type = other, "Unknown strategy type in optimizer");
            None
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use mm_core::types::PriceLevel;
    use rust_decimal_macros::dec;

    #[test]
    fn cartesian_product_generates_all_combinations() {
        let ranges = vec![
            ParameterRange {
                name: "spread_bps".to_string(),
                values: vec![
                    serde_json::json!(5),
                    serde_json::json!(10),
                ],
            },
            ParameterRange {
                name: "num_levels".to_string(),
                values: vec![
                    serde_json::json!(2),
                    serde_json::json!(3),
                    serde_json::json!(5),
                ],
            },
        ];

        let combos = cartesian_product(&ranges);
        // 2 spread_bps * 3 num_levels = 6 combinations
        assert_eq!(combos.len(), 6);

        // Verify each combination has exactly 2 entries
        for combo in &combos {
            assert_eq!(combo.len(), 2);
            assert_eq!(combo[0].0, "spread_bps");
            assert_eq!(combo[1].0, "num_levels");
        }

        // Verify all combinations are present
        let spread_values: Vec<&serde_json::Value> =
            combos.iter().map(|c| &c[0].1).collect();
        assert!(spread_values.contains(&&serde_json::json!(5)));
        assert!(spread_values.contains(&&serde_json::json!(10)));

        let level_values: Vec<&serde_json::Value> =
            combos.iter().map(|c| &c[1].1).collect();
        assert!(level_values.contains(&&serde_json::json!(2)));
        assert!(level_values.contains(&&serde_json::json!(3)));
        assert!(level_values.contains(&&serde_json::json!(5)));
    }

    #[test]
    fn cartesian_product_empty_ranges() {
        let combos = cartesian_product(&[]);
        // Empty input produces a single empty combination
        assert_eq!(combos.len(), 1);
        assert!(combos[0].is_empty());
    }

    #[test]
    fn cartesian_product_single_range() {
        let ranges = vec![ParameterRange {
            name: "x".to_string(),
            values: vec![serde_json::json!(1), serde_json::json!(2)],
        }];
        let combos = cartesian_product(&ranges);
        assert_eq!(combos.len(), 2);
    }

    #[test]
    fn merge_params_overrides_base() {
        let base = serde_json::json!({
            "spread_bps": 10,
            "num_levels": 3,
            "order_size": "0.01"
        });

        let overrides = vec![
            ("spread_bps".to_string(), serde_json::json!(20)),
            ("num_levels".to_string(), serde_json::json!(5)),
        ];

        let merged = merge_params(&base, &overrides);

        assert_eq!(merged["spread_bps"], 20);
        assert_eq!(merged["num_levels"], 5);
        // Untouched param remains
        assert_eq!(merged["order_size"], "0.01");
    }

    #[test]
    fn merge_params_adds_new_keys() {
        let base = serde_json::json!({ "a": 1 });
        let overrides = vec![("b".to_string(), serde_json::json!(2))];

        let merged = merge_params(&base, &overrides);
        assert_eq!(merged["a"], 1);
        assert_eq!(merged["b"], 2);
    }

    #[test]
    fn extract_metric_returns_correct_value() {
        // Create a minimal BacktestResult to test metric extraction.
        let result = BacktestResult {
            total_pnl: dec!(1500),
            total_fees: dec!(50),
            net_pnl: dec!(1450),
            num_trades: 10,
            num_fills: 20,
            win_rate: dec!(0.7),
            max_drawdown: dec!(200),
            max_drawdown_pct: dec!(2),
            sharpe_ratio: dec!(1.5),
            sortino_ratio: dec!(2.3),
            avg_trade_pnl: dec!(145),
            profit_factor: dec!(3.5),
            total_volume: dec!(1000000),
            start_time: Utc::now(),
            end_time: Utc::now(),
            duration: chrono::Duration::hours(1),
            equity_curve: vec![],
        };

        assert!((extract_metric(&result, &OptimizationMetric::Sharpe) - 1.5).abs() < 0.001);
        assert!((extract_metric(&result, &OptimizationMetric::Sortino) - 2.3).abs() < 0.001);
        assert!((extract_metric(&result, &OptimizationMetric::TotalPnl) - 1500.0).abs() < 0.001);
        assert!((extract_metric(&result, &OptimizationMetric::MaxDrawdown) - 200.0).abs() < 0.001);
        assert!((extract_metric(&result, &OptimizationMetric::ProfitFactor) - 3.5).abs() < 0.001);
        assert!((extract_metric(&result, &OptimizationMetric::WinRate) - 0.7).abs() < 0.001);
    }

    #[test]
    fn create_strategy_spread() {
        let params = serde_json::json!({
            "spread_bps": 10,
            "order_size": "0.01",
            "num_levels": 3
        });
        let strategy = create_strategy("spread", &params);
        assert!(strategy.is_some());
        let name = strategy.unwrap().into_inner().name().to_string();
        assert!(
            name.to_lowercase().contains("spread"),
            "Expected strategy name to contain 'spread', got '{}'",
            name
        );
    }

    #[test]
    fn create_strategy_unknown_type_returns_none() {
        let params = serde_json::json!({});
        let strategy = create_strategy("nonexistent", &params);
        assert!(strategy.is_none());
    }

    /// Generate synthetic order book data for optimizer tests.
    fn generate_test_events(count: usize) -> Vec<MarketEvent> {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut events = Vec::with_capacity(count);
        let exchange = Exchange::Simulated;
        let symbol = Symbol::new("BTC", "USDT");
        let base_time = Utc::now() - chrono::Duration::seconds(count as i64);
        let mut mid_price: f64 = 50_000.0;
        let mut seed: u64 = 42;

        for i in 0..count {
            let ts = base_time + chrono::Duration::seconds(i as i64);
            seed = {
                let mut hasher = DefaultHasher::new();
                seed.hash(&mut hasher);
                i.hash(&mut hasher);
                hasher.finish()
            };
            let step_pct = ((seed % 1000) as f64 / 1000.0 - 0.5) * 0.005;
            mid_price *= 1.0 + step_pct;
            if mid_price < 100.0 {
                mid_price = 100.0;
            }

            let spread_bps = 1.0 + (seed % 10) as f64;
            let half_spread = mid_price * spread_bps / 10_000.0 / 2.0;
            let best_bid = mid_price - half_spread;
            let best_ask = mid_price + half_spread;

            let bids = vec![PriceLevel {
                price: Decimal::from_f64_retain(best_bid)
                    .unwrap_or(dec!(50000))
                    .round_dp(2),
                quantity: dec!(10),
            }];
            let asks = vec![PriceLevel {
                price: Decimal::from_f64_retain(best_ask)
                    .unwrap_or(dec!(50000))
                    .round_dp(2),
                quantity: dec!(10),
            }];

            events.push(MarketEvent::OrderBookSnapshot {
                timestamp: ts,
                exchange,
                symbol: symbol.clone(),
                bids,
                asks,
                sequence: i as u64,
            });
        }

        events
    }

    #[test]
    fn optimizer_runs_and_returns_sorted_results() {
        let events = generate_test_events(200);

        let config = OptimizationConfig {
            strategy_type: "spread".to_string(),
            base_params: serde_json::json!({
                "order_size": "0.01"
            }),
            param_ranges: vec![
                ParameterRange {
                    name: "spread_bps".to_string(),
                    values: vec![
                        serde_json::json!(5),
                        serde_json::json!(10),
                        serde_json::json!(20),
                    ],
                },
                ParameterRange {
                    name: "num_levels".to_string(),
                    values: vec![
                        serde_json::json!(2),
                        serde_json::json!(3),
                    ],
                },
            ],
            backtest_config: BacktestConfig::default(),
            metric: OptimizationMetric::TotalPnl,
            top_n: 3,
        };

        let results = ParameterOptimizer::run(&config, events, None);

        // 3 spread_bps * 2 num_levels = 6 combos, top_n = 3
        assert!(results.len() <= 3);
        assert!(!results.is_empty());

        // Verify results are sorted in descending order by metric
        for window in results.windows(2) {
            assert!(
                window[0].metric_value >= window[1].metric_value,
                "Results should be sorted descending by metric. Got {} before {}",
                window[0].metric_value,
                window[1].metric_value
            );
        }
    }

    #[test]
    fn optimizer_max_drawdown_sorted_ascending() {
        let events = generate_test_events(200);

        let config = OptimizationConfig {
            strategy_type: "spread".to_string(),
            base_params: serde_json::json!({
                "order_size": "0.01"
            }),
            param_ranges: vec![ParameterRange {
                name: "spread_bps".to_string(),
                values: vec![
                    serde_json::json!(5),
                    serde_json::json!(10),
                    serde_json::json!(20),
                ],
            }],
            backtest_config: BacktestConfig::default(),
            metric: OptimizationMetric::MaxDrawdown,
            top_n: 10,
        };

        let results = ParameterOptimizer::run(&config, events, None);

        // MaxDrawdown should be sorted ascending (lower drawdown is better)
        for window in results.windows(2) {
            assert!(
                window[0].metric_value <= window[1].metric_value,
                "MaxDrawdown results should be sorted ascending. Got {} before {}",
                window[0].metric_value,
                window[1].metric_value
            );
        }
    }

    #[test]
    fn optimizer_with_progress_channel() {
        let events = generate_test_events(100);

        let config = OptimizationConfig {
            strategy_type: "spread".to_string(),
            base_params: serde_json::json!({
                "order_size": "0.01",
                "spread_bps": 10
            }),
            param_ranges: vec![ParameterRange {
                name: "num_levels".to_string(),
                values: vec![
                    serde_json::json!(2),
                    serde_json::json!(3),
                ],
            }],
            backtest_config: BacktestConfig::default(),
            metric: OptimizationMetric::Sharpe,
            top_n: 5,
        };

        let (tx, mut rx) = tokio::sync::mpsc::channel(16);

        let results = ParameterOptimizer::run(&config, events, Some(tx));

        assert!(!results.is_empty());

        // Check that progress messages were sent
        let mut progress_count = 0;
        while let Ok(progress) = rx.try_recv() {
            progress_count += 1;
            assert_eq!(progress.total, 2);
            assert!(progress.completed <= 2);
        }
        assert!(progress_count > 0, "Expected at least one progress message");
    }
}
