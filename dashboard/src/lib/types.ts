// Matches crates/mm-core/src/types.rs

export type Exchange =
  | "Binance"
  | "Coinbase"
  | "Kraken"
  | "Uniswap"
  | "Raydium"
  | "InteractiveBrokers"
  | "Alpaca"
  | "Simulated";

export type Side = "Buy" | "Sell";
export type OrderType = "Market" | "Limit" | "StopLimit" | "StopMarket";
export type TimeInForce = "GTC" | "IOC" | "FOK" | "GTD";
export type OrderStatus =
  | "Pending" | "Submitted" | "New" | "PartiallyFilled"
  | "Filled" | "PendingCancel" | "Cancelled" | "Rejected" | "Expired";

export interface Symbol {
  base: string;
  quote: string;
}

export interface PriceLevel {
  price: string;
  quantity: string;
}

export interface OrderBook {
  exchange: Exchange;
  symbol: Symbol;
  bids: PriceLevel[];
  asks: PriceLevel[];
  timestamp: string;
  sequence: number;
}

export interface Order {
  client_order_id: string;
  exchange_order_id: string | null;
  exchange: Exchange;
  symbol: Symbol;
  side: Side;
  order_type: OrderType;
  time_in_force: TimeInForce;
  price: string | null;
  quantity: string;
  status: OrderStatus;
  filled_quantity: string;
  remaining_quantity: string;
  average_fill_price: string;
  strategy_id: string;
  reduce_only: boolean;
  created_at: string;
  updated_at: string;
}

export interface Position {
  exchange: Exchange;
  symbol: Symbol;
  quantity: string;
  average_entry_price: string;
  unrealized_pnl: string;
  realized_pnl: string;
  updated_at: string;
}

export interface PortfolioRisk {
  total_var_95: string;
  total_var_99: string;
  net_exposure: string;
  gross_exposure: string;
  max_drawdown: string;
  daily_pnl: string;
  positions: Position[];
  timestamp: string;
}

export interface StrategyConfig {
  strategy_id: string;
  strategy_type: string;
  exchange: Exchange;
  symbol: Symbol;
  enabled: boolean;
  params: Record<string, unknown>;
}

export interface StrategyStatus {
  config: StrategyConfig;
  running: boolean;
  started_at: string | null;
  stopped_at: string | null;
}

export interface StrategyRiskSummary {
  posture: string;
  current_total_pnl: string | null;
  upside: string[];
  downside: string[];
}

export interface StrategyLifecycleInfo {
  can_start: boolean;
  can_stop: boolean;
  can_edit: boolean;
  save_mode: string;
  start_behavior: string;
  stop_behavior: string;
}

export interface StrategyDetail {
  status: StrategyStatus;
  metrics: StrategyMetrics | null;
  risk_summary: StrategyRiskSummary;
  lifecycle: StrategyLifecycleInfo;
}

export interface HealthResponse {
  status: string;
  timestamp: string;
  version: string;
}

export interface StrategyMetrics {
  strategy_id: string;
  realized_pnl: string;
  unrealized_pnl: string;
  current_position: string;
  peak_total_pnl: string;
  max_drawdown: string;
  total_fills: number;
  total_orders_placed: number;
  total_orders_cancelled: number;
  total_orders_rejected: number;
  avg_spread_captured_bps: string;
  volume_traded: string;
  uptime_secs: number;
  last_fill_at: string | null;
}

export interface KillSwitchResponse {
  active: boolean;
  timestamp: string;
}

export interface ApiResponse<T> {
  status: "ok" | "error";
  data?: T;
  error?: string;
}

// WebSocket server -> client messages (tagged union on "type")
export type ServerMessage =
  | PositionUpdateMsg
  | OrderUpdateMsg
  | RiskUpdateMsg
  | MarketDataTickMsg
  | StrategyMetricsMsg
  | StrategyLifecycleEventMsg
  | ErrorMsg
  | SubscribedMsg
  | UnsubscribedMsg;

export interface PositionUpdateMsg {
  type: "PositionUpdate";
  exchange: Exchange;
  symbol: Symbol;
  quantity: string;
  unrealized_pnl: string;
  realized_pnl: string;
}

export interface OrderUpdateMsg {
  type: "OrderUpdate";
  client_order_id: string;
  exchange: Exchange;
  symbol: Symbol;
  status: string;
  filled_quantity: string;
  remaining_quantity: string;
}

export interface RiskUpdateMsg {
  type: "RiskUpdate";
  net_exposure: string;
  gross_exposure: string;
  daily_pnl: string;
  kill_switch_active: boolean;
}

export interface MarketDataTickMsg {
  type: "MarketDataTick";
  exchange: Exchange;
  symbol: Symbol;
  best_bid: string;
  best_ask: string;
  mid_price: string;
  spread_bps: string;
}

export interface ErrorMsg {
  type: "Error";
  message: string;
}

export interface SubscribedMsg {
  type: "Subscribed";
  channel: string;
}

export interface UnsubscribedMsg {
  type: "Unsubscribed";
  channel: string;
}

export interface StrategyMetricsMsg {
  type: "StrategyMetrics";
  strategy_id: string;
  metrics: StrategyMetrics;
}

export interface StrategyLifecycleEventMsg {
  type: "StrategyLifecycleEvent";
  strategy_id: string;
  event: string;
  running: boolean;
  timestamp: string;
}

export type ClientMessage =
  | { action: "subscribe"; channel: string }
  | { action: "unsubscribe"; channel: string }
  | { action: "ping" };

// ---------------------------------------------------------------------------
// Strategy Type Descriptors & Presets
// ---------------------------------------------------------------------------

export interface StrategyField {
  name: string;
  label: string;
  type: "number" | "select" | "boolean" | "json";
  default: unknown;
  min?: number;
  max?: number;
  step?: number;
  tooltip?: string;
  options?: { label: string; value: string }[];
}

export interface StrategyTypeInfo {
  type: string;
  label: string;
  category: string;
  description: string;
  howItWorks: string;
  bestFor: string;
  watchFor: string;
  paperFit: string;
  creationMode: "single_symbol" | "template_only";
  creationHint?: string;
  defaultParams: Record<string, unknown>;
  fields: StrategyField[];
}

export interface StrategyPreset {
  label: string;
  params: Record<string, unknown>;
}

export function normalizeStrategyType(type: string): string {
  return type === "avellaneda_stoikov" ? "avellaneda" : type;
}

export function getStrategyTypeInfo(strategyType: string): StrategyTypeInfo | undefined {
  const normalized = normalizeStrategyType(strategyType);
  return STRATEGY_TYPES.find((strategy) => strategy.type === normalized);
}

export interface OptimizationResult {
  params: Record<string, unknown>;
  backtest_result: {
    total_pnl: number;
    sharpe_ratio: number;
    sortino_ratio: number;
    max_drawdown_pct: number;
    profit_factor: number;
    win_rate: number;
    total_trades: number;
  };
  metric_value: number;
}

export interface BacktestResult {
  strategy_name: string;
  total_pnl: string;
  total_fees: string;
  net_pnl: string;
  num_trades: number;
  num_fills: number;
  win_rate: string;
  max_drawdown: string;
  max_drawdown_pct: string;
  sharpe_ratio: string;
  sortino_ratio: string;
  avg_trade_pnl: string;
  profit_factor: string;
  total_volume: string;
  start_time: string;
  end_time: string;
  duration: { secs: number; nanos: number };
  equity_curve: [string, string][];
}

export interface BacktestRequest {
  strategy_type: string;
  symbol: string;
  initial_capital?: string;
  num_events?: number;
  params?: Record<string, unknown>;
}

export interface PlaceOrderRequest {
  exchange: string;
  symbol: string;
  side: Side;
  order_type: OrderType;
  price?: string | null;
  quantity: string;
  time_in_force?: TimeInForce;
  strategy_id?: string;
  reduce_only?: boolean;
}

export const STRATEGY_TYPES: StrategyTypeInfo[] = [
  {
    type: "spread",
    label: "Spread Market Making",
    category: "Market Making",
    description: "Symmetric bid/ask orders around mid price",
    howItWorks: "Quotes both sides of the market around the mid price and refreshes when price moves or the quote gets stale.",
    bestFor: "Liquid names with stable spreads where you want to practice quoting, inventory flow, and order refresh behavior.",
    watchFor: "Can churn in fast moves and needs fresh ticks to keep quotes relevant.",
    paperFit: "Good connector test for Alpaca paper, but it depends more on steady live market data than the directional strategies.",
    creationMode: "single_symbol",
    fields: [
      { name: "spread_bps", label: "Spread (bps)", type: "number", default: 12, min: 1, max: 1000, step: 1, tooltip: "Distance from mid price in basis points" },
      { name: "order_size", label: "Order Size", type: "number", default: 1, min: 0.0001, step: 0.001, tooltip: "Quantity per level" },
      { name: "num_levels", label: "Number of Levels", type: "number", default: 1, min: 1, max: 20, step: 1, tooltip: "Price levels on each side" },
      { name: "refresh_interval_ms", label: "Refresh Interval (ms)", type: "number", default: 1000, min: 100, max: 60000, step: 100, tooltip: "Min time between order refreshes" },
      { name: "min_spread_bps", label: "Min Spread (bps)", type: "number", default: 6, min: 0, max: 500, step: 1, tooltip: "Floor spread in basis points" },
      { name: "reprice_threshold_bps", label: "Reprice Threshold (bps)", type: "number", default: 4, min: 1, max: 100, step: 1, tooltip: "Mid-price movement that triggers cancel-replace" },
    ],
    defaultParams: { spread_bps: "12", order_size: "1", num_levels: 1, refresh_interval_ms: 1000, min_spread_bps: "6", reprice_threshold_bps: "4" },
  },
  {
    type: "avellaneda",
    label: "Avellaneda-Stoikov",
    category: "Market Making",
    description: "Inventory-aware market making with reservation price model",
    howItWorks: "Shifts its reservation price based on inventory, volatility, and remaining session time so the strategy leans away from building too much position.",
    bestFor: "Advanced market-making experiments where inventory control matters more than simple symmetric quoting.",
    watchFor: "Needs calibration for volatility and liquidity inputs; poor tuning can produce quotes that are too wide or too aggressive.",
    paperFit: "Use after spread once you want a more model-driven market maker on Alpaca paper.",
    creationMode: "single_symbol",
    fields: [
      { name: "gamma", label: "Risk Aversion (gamma)", type: "number", default: 0.15, min: 0.001, max: 10, step: 0.01, tooltip: "Higher values penalize inventory more aggressively" },
      { name: "sigma", label: "Volatility (sigma)", type: "number", default: 0.02, min: 0.001, max: 1, step: 0.001, tooltip: "Annualized volatility estimate" },
      { name: "kappa", label: "Liquidity (kappa)", type: "number", default: 1.5, min: 0.1, max: 100, step: 0.1, tooltip: "Order book liquidity parameter" },
      { name: "order_size", label: "Order Size", type: "number", default: 1, min: 0.0001, step: 0.001, tooltip: "Quantity per side" },
      { name: "max_position", label: "Max Position", type: "number", default: 1.0, min: 0.001, step: 0.01, tooltip: "Maximum absolute inventory" },
      { name: "session_duration_secs", label: "Session Duration (s)", type: "number", default: 3600, min: 60, max: 86400, step: 60, tooltip: "Total session duration in seconds" },
    ],
    defaultParams: { gamma: "0.15", sigma: "0.02", kappa: "1.5", order_size: "1", max_position: "1", session_duration_secs: 3600 },
  },
  {
    type: "grid",
    label: "Grid Trading",
    category: "Market Making",
    description: "Arithmetic or geometric grid of buy/sell orders across a price range",
    howItWorks: "Pre-computes price levels across a range, buys below the market, sells above it, and replaces orders as fills move through the ladder.",
    bestFor: "Range-bound markets where you want repeated mean-reverting fills inside a known price band.",
    watchFor: "Performs poorly in one-way trends and needs a sensible upper/lower range before it starts.",
    paperFit: "Viable on Alpaca paper, but the starter ranges are templates that should be retuned to the current price regime.",
    creationMode: "single_symbol",
    fields: [
      { name: "upper_price", label: "Upper Price", type: "number", default: 250, min: 0.01, step: 0.01, tooltip: "Upper bound of the grid range" },
      { name: "lower_price", label: "Lower Price", type: "number", default: 150, min: 0.01, step: 0.01, tooltip: "Lower bound of the grid range" },
      { name: "num_grids", label: "Number of Grids", type: "number", default: 10, min: 2, max: 100, step: 1, tooltip: "Number of grid intervals" },
      { name: "order_size", label: "Order Size", type: "number", default: 1, min: 0.0001, step: 0.001, tooltip: "Quantity per grid level" },
      { name: "grid_type", label: "Grid Type", type: "select", default: "Arithmetic", options: [{ label: "Arithmetic", value: "Arithmetic" }, { label: "Geometric", value: "Geometric" }], tooltip: "Equal price spacing vs equal percentage spacing" },
    ],
    defaultParams: { upper_price: "250", lower_price: "150", num_grids: 10, order_size: "1", grid_type: "Arithmetic" },
  },
  {
    type: "twap",
    label: "TWAP Execution",
    category: "Execution",
    description: "Time-weighted average price execution over a fixed window",
    howItWorks: "Breaks a target order into equal time slices and works them at regular intervals across a fixed schedule.",
    bestFor: "Execution testing when you already know you want to buy or sell and care more about pacing than alpha generation.",
    watchFor: "Not an alpha strategy by itself; it will keep working the order even if the tape turns against you.",
    paperFit: "Useful for Alpaca paper when you want to test order slicing and pacing logic instead of signal generation.",
    creationMode: "single_symbol",
    fields: [
      { name: "target_quantity", label: "Target Quantity", type: "number", default: 1.0, min: 0.0001, step: 0.001, tooltip: "Total quantity to execute" },
      { name: "duration_secs", label: "Duration (s)", type: "number", default: 3600, min: 60, max: 86400, step: 60, tooltip: "Total execution window in seconds" },
      { name: "slice_interval_secs", label: "Slice Interval (s)", type: "number", default: 300, min: 5, max: 3600, step: 5, tooltip: "Interval between slices" },
      { name: "side", label: "Side", type: "select", default: "Buy", options: [{ label: "Buy", value: "Buy" }, { label: "Sell", value: "Sell" }], tooltip: "Direction of execution" },
      { name: "max_deviation_bps", label: "Max Deviation (bps)", type: "number", default: 10, min: 1, max: 500, step: 1, tooltip: "Maximum price deviation from mid in basis points" },
    ],
    defaultParams: { target_quantity: "1", duration_secs: 3600, slice_interval_secs: 300, side: "Buy", max_deviation_bps: "10" },
  },
  {
    type: "vwap",
    label: "VWAP Execution",
    category: "Execution",
    description: "Volume-weighted average price execution with historical volume profile",
    howItWorks: "Schedules a larger share of the order into buckets where you expect more market volume, instead of slicing evenly in time.",
    bestFor: "Execution experiments where you want a more market-aware schedule than TWAP.",
    watchFor: "Only as good as the volume profile you feed it; unrealistic weights make the schedule misleading.",
    paperFit: "Good second-step execution strategy on Alpaca paper after TWAP.",
    creationMode: "single_symbol",
    fields: [
      { name: "target_quantity", label: "Target Quantity", type: "number", default: 1.0, min: 0.0001, step: 0.001, tooltip: "Total quantity to execute" },
      { name: "duration_secs", label: "Duration (s)", type: "number", default: 3600, min: 60, max: 86400, step: 60, tooltip: "Total execution window in seconds" },
      { name: "side", label: "Side", type: "select", default: "Buy", options: [{ label: "Buy", value: "Buy" }, { label: "Sell", value: "Sell" }], tooltip: "Direction of execution" },
      { name: "volume_profile", label: "Volume Profile (JSON)", type: "json", default: [1, 2, 3, 2, 1], tooltip: "Array of relative volume weights per bucket" },
      { name: "max_deviation_bps", label: "Max Deviation (bps)", type: "number", default: 10, min: 1, max: 500, step: 1, tooltip: "Maximum price deviation from mid in basis points" },
    ],
    defaultParams: { target_quantity: "1", duration_secs: 3600, side: "Buy", volume_profile: [1, 2, 3, 2, 1], max_deviation_bps: "10" },
  },
  {
    type: "momentum",
    label: "Momentum",
    category: "Directional",
    description: "Trend-following strategy based on recent price momentum",
    howItWorks: "Measures recent price acceleration, enters with the move when momentum clears a threshold, and exits on weakening momentum or a trailing stop.",
    bestFor: "Trending sessions where you want to participate in breakouts instead of fading them.",
    watchFor: "Whipsaws badly in choppy markets and can overtrade if thresholds are too tight.",
    paperFit: "A reasonable Alpaca paper strategy once you want directional exposure with clearer signal timing than market making.",
    creationMode: "single_symbol",
    fields: [
      { name: "lookback_periods", label: "Lookback Periods", type: "number", default: 20, min: 2, max: 500, step: 1, tooltip: "Number of price observations in lookback window" },
      { name: "entry_threshold_bps", label: "Entry Threshold (bps)", type: "number", default: 50, min: 1, max: 1000, step: 1, tooltip: "Momentum threshold to trigger entry" },
      { name: "exit_threshold_bps", label: "Exit Threshold (bps)", type: "number", default: 15, min: 1, max: 500, step: 1, tooltip: "Momentum threshold for mean-reversion exit" },
      { name: "order_size", label: "Order Size", type: "number", default: 1, min: 0.0001, step: 0.001, tooltip: "Quantity for each entry/exit" },
      { name: "trailing_stop_bps", label: "Trailing Stop (bps)", type: "number", default: 30, min: 1, max: 1000, step: 1, tooltip: "Trailing stop distance in basis points" },
      { name: "max_position", label: "Max Position", type: "number", default: 1.0, min: 0.001, step: 0.01, tooltip: "Maximum absolute position size" },
    ],
    defaultParams: { lookback_periods: 20, entry_threshold_bps: "50", exit_threshold_bps: "15", order_size: "1", trailing_stop_bps: "30", max_position: "1" },
  },
  {
    type: "mean_reversion",
    label: "Mean Reversion",
    category: "Directional",
    description: "Bollinger Band-style strategy that trades against price extremes",
    howItWorks: "Builds a rolling mean and volatility band from recent mid prices, buys below the lower band, sells above the upper band, and exits on reversion.",
    bestFor: "Single-symbol paper trading on liquid names that spend time oscillating around an intraday fair value.",
    watchFor: "Can fight strong trends, and it naturally stays idle when price never stretches far enough from its rolling mean.",
    paperFit: "Best first directional strategy for the current Alpaca dashboard flow because it is single-symbol, easy to tune, and safe to leave disabled by default.",
    creationMode: "single_symbol",
    fields: [
      { name: "window_periods", label: "Window Periods", type: "number", default: 30, min: 2, max: 500, step: 1, tooltip: "Number of observations in rolling window" },
      { name: "num_std_dev", label: "Std Deviations", type: "number", default: 2.5, min: 0.5, max: 5, step: 0.1, tooltip: "Band width in standard deviations" },
      { name: "order_size", label: "Order Size", type: "number", default: 1, min: 0.0001, step: 0.001, tooltip: "Quantity for each entry/exit" },
      { name: "max_position", label: "Max Position", type: "number", default: 1.0, min: 0.001, step: 0.01, tooltip: "Maximum absolute position size" },
      { name: "exit_at_mean", label: "Exit at Mean", type: "boolean", default: true, tooltip: "Exit when price reverts to mean (vs opposite band)" },
    ],
    defaultParams: { window_periods: 30, num_std_dev: "2.5", order_size: "1", max_position: "1", exit_at_mean: true },
  },
  {
    type: "stat_arb",
    label: "Statistical Arbitrage",
    category: "Relative Value",
    description: "Pairs trading via z-score of spread between two correlated instruments",
    howItWorks: "Tracks the spread between two related instruments, enters when that spread deviates far from its rolling average, and exits as it converges.",
    bestFor: "Advanced relative-value setups where you can monitor both legs and care about spread behavior more than outright direction.",
    watchFor: "Requires two-leg thinking, correlation can break, and the current UI is less natural for configuring it than the single-symbol strategies.",
    paperFit: "Supported in the engine, but for now you start it from the seeded Alpaca paper template instead of building it in the single-symbol form.",
    creationMode: "template_only",
    creationHint: "Use the seeded AAPL/USD vs MSFT/USD template from the Strategies panel until a dedicated pair editor exists.",
    fields: [
      { name: "lookback_periods", label: "Lookback Periods", type: "number", default: 50, min: 5, max: 500, step: 1, tooltip: "Number of spread observations in rolling window" },
      { name: "entry_z_score", label: "Entry Z-Score", type: "number", default: 2.0, min: 0.5, max: 5, step: 0.1, tooltip: "Z-score threshold to enter a position" },
      { name: "exit_z_score", label: "Exit Z-Score", type: "number", default: 0.5, min: 0, max: 3, step: 0.1, tooltip: "Z-score threshold to exit (less than entry)" },
      { name: "order_size", label: "Order Size", type: "number", default: 1, min: 0.0001, step: 0.001, tooltip: "Quantity for each entry/exit" },
      { name: "max_position", label: "Max Position", type: "number", default: 1.0, min: 0.001, step: 0.01, tooltip: "Maximum absolute position size" },
    ],
    defaultParams: { lookback_periods: 50, entry_z_score: "2.0", exit_z_score: "0.5", order_size: "1", max_position: "1" },
  },
];

export const STRATEGY_PRESETS: Record<string, StrategyPreset[]> = {
  spread: [
    { label: "Conservative", params: { spread_bps: "20", order_size: "1", num_levels: 1, refresh_interval_ms: 2000, min_spread_bps: "10", reprice_threshold_bps: "6" } },
    { label: "Moderate", params: { spread_bps: "12", order_size: "1", num_levels: 1, refresh_interval_ms: 1000, min_spread_bps: "6", reprice_threshold_bps: "4" } },
    { label: "Aggressive", params: { spread_bps: "8", order_size: "1", num_levels: 2, refresh_interval_ms: 750, min_spread_bps: "4", reprice_threshold_bps: "3" } },
  ],
  avellaneda: [
    { label: "Conservative", params: { gamma: "0.3", sigma: "0.015", kappa: "1.5", order_size: "1", max_position: "1", session_duration_secs: 3600 } },
    { label: "Moderate", params: { gamma: "0.15", sigma: "0.02", kappa: "1.5", order_size: "1", max_position: "1", session_duration_secs: 3600 } },
    { label: "Aggressive", params: { gamma: "0.08", sigma: "0.03", kappa: "2.0", order_size: "1", max_position: "1", session_duration_secs: 7200 } },
  ],
  grid: [
    { label: "Tight Range", params: { upper_price: "210", lower_price: "190", num_grids: 6, order_size: "1", grid_type: "Arithmetic" } },
    { label: "Standard Range", params: { upper_price: "225", lower_price: "175", num_grids: 10, order_size: "1", grid_type: "Arithmetic" } },
    { label: "Wide Range", params: { upper_price: "250", lower_price: "150", num_grids: 12, order_size: "1", grid_type: "Geometric" } },
  ],
  twap: [
    { label: "Quick (15 min)", params: { target_quantity: "1", duration_secs: 900, slice_interval_secs: 60, side: "Buy", max_deviation_bps: "15" } },
    { label: "Standard (1 hr)", params: { target_quantity: "1", duration_secs: 3600, slice_interval_secs: 300, side: "Buy", max_deviation_bps: "10" } },
    { label: "Extended (4 hr)", params: { target_quantity: "1", duration_secs: 14400, slice_interval_secs: 900, side: "Buy", max_deviation_bps: "5" } },
  ],
  vwap: [
    { label: "Quick (15 min)", params: { target_quantity: "1", duration_secs: 900, side: "Buy", volume_profile: [1, 2, 3, 2, 1], max_deviation_bps: "15" } },
    { label: "Standard (1 hr)", params: { target_quantity: "1", duration_secs: 3600, side: "Buy", volume_profile: [1, 2, 3, 4, 3, 2, 1], max_deviation_bps: "10" } },
    { label: "Extended (4 hr)", params: { target_quantity: "1", duration_secs: 14400, side: "Buy", volume_profile: [1, 1, 2, 3, 4, 4, 3, 2, 1, 1], max_deviation_bps: "5" } },
  ],
  momentum: [
    { label: "Conservative", params: { lookback_periods: 30, entry_threshold_bps: "80", exit_threshold_bps: "20", order_size: "1", trailing_stop_bps: "60", max_position: "1" } },
    { label: "Moderate", params: { lookback_periods: 20, entry_threshold_bps: "50", exit_threshold_bps: "15", order_size: "1", trailing_stop_bps: "30", max_position: "1" } },
    { label: "Aggressive", params: { lookback_periods: 10, entry_threshold_bps: "30", exit_threshold_bps: "10", order_size: "1", trailing_stop_bps: "20", max_position: "1" } },
  ],
  mean_reversion: [
    { label: "Conservative", params: { window_periods: 30, num_std_dev: "2.5", order_size: "1", max_position: "1", exit_at_mean: true } },
    { label: "Moderate", params: { window_periods: 20, num_std_dev: "2.0", order_size: "1", max_position: "1", exit_at_mean: true } },
    { label: "Aggressive", params: { window_periods: 10, num_std_dev: "1.5", order_size: "1", max_position: "1", exit_at_mean: false } },
  ],
  stat_arb: [
    { label: "Conservative", params: { lookback_periods: 100, entry_z_score: "2.5", exit_z_score: "0.5", order_size: "1", max_position: "1" } },
    { label: "Moderate", params: { lookback_periods: 50, entry_z_score: "2.0", exit_z_score: "0.5", order_size: "1", max_position: "1" } },
    { label: "Aggressive", params: { lookback_periods: 20, entry_z_score: "1.5", exit_z_score: "0.3", order_size: "1", max_position: "1" } },
  ],
};

export const CREATABLE_STRATEGY_TYPES = STRATEGY_TYPES.filter(
  (strategy) => strategy.creationMode === "single_symbol",
);
