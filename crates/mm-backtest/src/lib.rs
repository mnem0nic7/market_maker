pub mod replay;
pub mod report;
pub mod sim_exchange;

// Re-export key types for convenient access
pub use replay::{BacktestConfig, MarketEvent, ReplayEngine};
pub use report::{BacktestResult, PerformanceCalculator, print_report};
pub use sim_exchange::{BalanceTracker, FeeModel, SimulatedExchange, SlippageModel};
