pub mod data_loader;
pub mod optimizer;
pub mod replay;
pub mod report;
pub mod sim_exchange;

// Re-export key types for convenient access
pub use data_loader::{download_binance_klines, download_binance_trades, load_csv};
pub use optimizer::{
    OptimizationConfig, OptimizationMetric, OptimizationProgress, OptimizationResult,
    ParameterOptimizer, ParameterRange,
};
pub use replay::{BacktestConfig, MarketEvent, ReplayEngine};
pub use report::{
    print_report, BacktestComparison, BacktestResult, BacktestResultStore, BacktestResultSummary,
    PerformanceCalculator,
};
pub use sim_exchange::{BalanceTracker, FeeModel, SimulatedExchange, SlippageModel};
