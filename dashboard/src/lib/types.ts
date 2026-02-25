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

export interface HealthResponse {
  status: string;
  timestamp: string;
  version: string;
}

export interface StrategyMetrics {
  strategy_id: string;
  realized_pnl: string;
  unrealized_pnl: string;
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
  description: string;
  defaultParams: Record<string, unknown>;
  fields: StrategyField[];
}

export interface StrategyPreset {
  label: string;
  params: Record<string, unknown>;
}

export const STRATEGY_TYPES: StrategyTypeInfo[] = [
  {
    type: "spread",
    label: "Spread Market Making",
    description: "Symmetric bid/ask orders around mid price",
    fields: [
      { name: "spread_bps", label: "Spread (bps)", type: "number", default: 10, min: 1, max: 1000, step: 1, tooltip: "Distance from mid price in basis points" },
      { name: "order_size", label: "Order Size", type: "number", default: 0.01, min: 0.0001, step: 0.001, tooltip: "Quantity per level" },
      { name: "num_levels", label: "Number of Levels", type: "number", default: 3, min: 1, max: 20, step: 1, tooltip: "Price levels on each side" },
      { name: "refresh_interval_ms", label: "Refresh Interval (ms)", type: "number", default: 1000, min: 100, max: 60000, step: 100, tooltip: "Min time between order refreshes" },
      { name: "min_spread_bps", label: "Min Spread (bps)", type: "number", default: 5, min: 0, max: 500, step: 1, tooltip: "Floor spread in basis points" },
      { name: "reprice_threshold_bps", label: "Reprice Threshold (bps)", type: "number", default: 3, min: 1, max: 100, step: 1, tooltip: "Mid-price movement that triggers cancel-replace" },
    ],
    defaultParams: { spread_bps: "10", order_size: "0.01", num_levels: 3, refresh_interval_ms: 1000, min_spread_bps: "5", reprice_threshold_bps: "3" },
  },
  {
    type: "avellaneda_stoikov",
    label: "Avellaneda-Stoikov",
    description: "Inventory-aware market making with reservation price model",
    fields: [
      { name: "gamma", label: "Risk Aversion (gamma)", type: "number", default: 0.1, min: 0.001, max: 10, step: 0.01, tooltip: "Higher values penalize inventory more aggressively" },
      { name: "sigma", label: "Volatility (sigma)", type: "number", default: 0.02, min: 0.001, max: 1, step: 0.001, tooltip: "Annualized volatility estimate" },
      { name: "kappa", label: "Liquidity (kappa)", type: "number", default: 1.5, min: 0.1, max: 100, step: 0.1, tooltip: "Order book liquidity parameter" },
      { name: "order_size", label: "Order Size", type: "number", default: 0.01, min: 0.0001, step: 0.001, tooltip: "Quantity per side" },
      { name: "max_position", label: "Max Position", type: "number", default: 1.0, min: 0.001, step: 0.01, tooltip: "Maximum absolute inventory" },
      { name: "session_duration_secs", label: "Session Duration (s)", type: "number", default: 3600, min: 60, max: 86400, step: 60, tooltip: "Total session duration in seconds" },
    ],
    defaultParams: { gamma: "0.1", sigma: "0.02", kappa: "1.5", order_size: "0.01", max_position: "1.0", session_duration_secs: 3600 },
  },
  {
    type: "grid",
    label: "Grid Trading",
    description: "Arithmetic or geometric grid of buy/sell orders across a price range",
    fields: [
      { name: "upper_price", label: "Upper Price", type: "number", default: 50000, min: 0.01, step: 0.01, tooltip: "Upper bound of the grid range" },
      { name: "lower_price", label: "Lower Price", type: "number", default: 40000, min: 0.01, step: 0.01, tooltip: "Lower bound of the grid range" },
      { name: "num_grids", label: "Number of Grids", type: "number", default: 10, min: 2, max: 100, step: 1, tooltip: "Number of grid intervals" },
      { name: "order_size", label: "Order Size", type: "number", default: 0.01, min: 0.0001, step: 0.001, tooltip: "Quantity per grid level" },
      { name: "grid_type", label: "Grid Type", type: "select", default: "Arithmetic", options: [{ label: "Arithmetic", value: "Arithmetic" }, { label: "Geometric", value: "Geometric" }], tooltip: "Equal price spacing vs equal percentage spacing" },
    ],
    defaultParams: { upper_price: "50000", lower_price: "40000", num_grids: 10, order_size: "0.01", grid_type: "Arithmetic" },
  },
  {
    type: "twap",
    label: "TWAP Execution",
    description: "Time-weighted average price execution over a fixed window",
    fields: [
      { name: "target_quantity", label: "Target Quantity", type: "number", default: 1.0, min: 0.0001, step: 0.001, tooltip: "Total quantity to execute" },
      { name: "duration_secs", label: "Duration (s)", type: "number", default: 3600, min: 60, max: 86400, step: 60, tooltip: "Total execution window in seconds" },
      { name: "slice_interval_secs", label: "Slice Interval (s)", type: "number", default: 60, min: 5, max: 3600, step: 5, tooltip: "Interval between slices" },
      { name: "side", label: "Side", type: "select", default: "Buy", options: [{ label: "Buy", value: "Buy" }, { label: "Sell", value: "Sell" }], tooltip: "Direction of execution" },
      { name: "max_deviation_bps", label: "Max Deviation (bps)", type: "number", default: 10, min: 1, max: 500, step: 1, tooltip: "Maximum price deviation from mid in basis points" },
    ],
    defaultParams: { target_quantity: "1.0", duration_secs: 3600, slice_interval_secs: 60, side: "Buy", max_deviation_bps: "10" },
  },
  {
    type: "vwap",
    label: "VWAP Execution",
    description: "Volume-weighted average price execution with historical volume profile",
    fields: [
      { name: "target_quantity", label: "Target Quantity", type: "number", default: 1.0, min: 0.0001, step: 0.001, tooltip: "Total quantity to execute" },
      { name: "duration_secs", label: "Duration (s)", type: "number", default: 3600, min: 60, max: 86400, step: 60, tooltip: "Total execution window in seconds" },
      { name: "side", label: "Side", type: "select", default: "Buy", options: [{ label: "Buy", value: "Buy" }, { label: "Sell", value: "Sell" }], tooltip: "Direction of execution" },
      { name: "volume_profile", label: "Volume Profile (JSON)", type: "json", default: [1, 2, 3, 2, 1], tooltip: "Array of relative volume weights per bucket" },
      { name: "max_deviation_bps", label: "Max Deviation (bps)", type: "number", default: 10, min: 1, max: 500, step: 1, tooltip: "Maximum price deviation from mid in basis points" },
    ],
    defaultParams: { target_quantity: "1.0", duration_secs: 3600, side: "Buy", volume_profile: [1, 2, 3, 2, 1], max_deviation_bps: "10" },
  },
  {
    type: "momentum",
    label: "Momentum",
    description: "Trend-following strategy based on recent price momentum",
    fields: [
      { name: "lookback_periods", label: "Lookback Periods", type: "number", default: 20, min: 2, max: 500, step: 1, tooltip: "Number of price observations in lookback window" },
      { name: "entry_threshold_bps", label: "Entry Threshold (bps)", type: "number", default: 50, min: 1, max: 1000, step: 1, tooltip: "Momentum threshold to trigger entry" },
      { name: "exit_threshold_bps", label: "Exit Threshold (bps)", type: "number", default: 10, min: 1, max: 500, step: 1, tooltip: "Momentum threshold for mean-reversion exit" },
      { name: "order_size", label: "Order Size", type: "number", default: 0.01, min: 0.0001, step: 0.001, tooltip: "Quantity for each entry/exit" },
      { name: "trailing_stop_bps", label: "Trailing Stop (bps)", type: "number", default: 30, min: 1, max: 1000, step: 1, tooltip: "Trailing stop distance in basis points" },
      { name: "max_position", label: "Max Position", type: "number", default: 1.0, min: 0.001, step: 0.01, tooltip: "Maximum absolute position size" },
    ],
    defaultParams: { lookback_periods: 20, entry_threshold_bps: "50", exit_threshold_bps: "10", order_size: "0.01", trailing_stop_bps: "30", max_position: "1.0" },
  },
  {
    type: "mean_reversion",
    label: "Mean Reversion",
    description: "Bollinger Band-style strategy that trades against price extremes",
    fields: [
      { name: "window_periods", label: "Window Periods", type: "number", default: 20, min: 2, max: 500, step: 1, tooltip: "Number of observations in rolling window" },
      { name: "num_std_dev", label: "Std Deviations", type: "number", default: 2.0, min: 0.5, max: 5, step: 0.1, tooltip: "Band width in standard deviations" },
      { name: "order_size", label: "Order Size", type: "number", default: 0.01, min: 0.0001, step: 0.001, tooltip: "Quantity for each entry/exit" },
      { name: "max_position", label: "Max Position", type: "number", default: 1.0, min: 0.001, step: 0.01, tooltip: "Maximum absolute position size" },
      { name: "exit_at_mean", label: "Exit at Mean", type: "boolean", default: true, tooltip: "Exit when price reverts to mean (vs opposite band)" },
    ],
    defaultParams: { window_periods: 20, num_std_dev: "2.0", order_size: "0.01", max_position: "1.0", exit_at_mean: true },
  },
  {
    type: "stat_arb",
    label: "Statistical Arbitrage",
    description: "Pairs trading via z-score of spread between two correlated instruments",
    fields: [
      { name: "lookback_periods", label: "Lookback Periods", type: "number", default: 50, min: 5, max: 500, step: 1, tooltip: "Number of spread observations in rolling window" },
      { name: "entry_z_score", label: "Entry Z-Score", type: "number", default: 2.0, min: 0.5, max: 5, step: 0.1, tooltip: "Z-score threshold to enter a position" },
      { name: "exit_z_score", label: "Exit Z-Score", type: "number", default: 0.5, min: 0, max: 3, step: 0.1, tooltip: "Z-score threshold to exit (less than entry)" },
      { name: "order_size", label: "Order Size", type: "number", default: 0.01, min: 0.0001, step: 0.001, tooltip: "Quantity for each entry/exit" },
      { name: "max_position", label: "Max Position", type: "number", default: 1.0, min: 0.001, step: 0.01, tooltip: "Maximum absolute position size" },
    ],
    defaultParams: { lookback_periods: 50, entry_z_score: "2.0", exit_z_score: "0.5", order_size: "0.01", max_position: "1.0" },
  },
];

export const STRATEGY_PRESETS: Record<string, StrategyPreset[]> = {
  spread: [
    { label: "Conservative", params: { spread_bps: "20", order_size: "0.005", num_levels: 2, refresh_interval_ms: 2000, min_spread_bps: "10", reprice_threshold_bps: "5" } },
    { label: "Moderate", params: { spread_bps: "10", order_size: "0.01", num_levels: 3, refresh_interval_ms: 1000, min_spread_bps: "5", reprice_threshold_bps: "3" } },
    { label: "Aggressive", params: { spread_bps: "5", order_size: "0.02", num_levels: 5, refresh_interval_ms: 500, min_spread_bps: "2", reprice_threshold_bps: "2" } },
  ],
  avellaneda_stoikov: [
    { label: "Conservative", params: { gamma: "0.3", sigma: "0.02", kappa: "1.5", order_size: "0.005", max_position: "0.5", session_duration_secs: 3600 } },
    { label: "Moderate", params: { gamma: "0.1", sigma: "0.02", kappa: "1.5", order_size: "0.01", max_position: "1.0", session_duration_secs: 3600 } },
    { label: "Aggressive", params: { gamma: "0.05", sigma: "0.03", kappa: "2.0", order_size: "0.02", max_position: "2.0", session_duration_secs: 7200 } },
  ],
  grid: [
    { label: "Tight Range", params: { upper_price: "45000", lower_price: "43000", num_grids: 20, order_size: "0.005", grid_type: "Arithmetic" } },
    { label: "Medium Range", params: { upper_price: "50000", lower_price: "40000", num_grids: 10, order_size: "0.01", grid_type: "Arithmetic" } },
    { label: "Wide Range", params: { upper_price: "60000", lower_price: "30000", num_grids: 15, order_size: "0.01", grid_type: "Geometric" } },
  ],
  twap: [
    { label: "Quick (15 min)", params: { target_quantity: "0.5", duration_secs: 900, slice_interval_secs: 30, side: "Buy", max_deviation_bps: "15" } },
    { label: "Standard (1 hr)", params: { target_quantity: "1.0", duration_secs: 3600, slice_interval_secs: 60, side: "Buy", max_deviation_bps: "10" } },
    { label: "Extended (4 hr)", params: { target_quantity: "5.0", duration_secs: 14400, slice_interval_secs: 120, side: "Buy", max_deviation_bps: "5" } },
  ],
  vwap: [
    { label: "Quick (15 min)", params: { target_quantity: "0.5", duration_secs: 900, side: "Buy", volume_profile: [1, 2, 3, 2, 1], max_deviation_bps: "15" } },
    { label: "Standard (1 hr)", params: { target_quantity: "1.0", duration_secs: 3600, side: "Buy", volume_profile: [1, 2, 3, 4, 3, 2, 1], max_deviation_bps: "10" } },
    { label: "Extended (4 hr)", params: { target_quantity: "5.0", duration_secs: 14400, side: "Buy", volume_profile: [1, 1, 2, 3, 4, 4, 3, 2, 1, 1], max_deviation_bps: "5" } },
  ],
  momentum: [
    { label: "Conservative", params: { lookback_periods: 30, entry_threshold_bps: "80", exit_threshold_bps: "20", order_size: "0.005", trailing_stop_bps: "50", max_position: "0.5" } },
    { label: "Moderate", params: { lookback_periods: 20, entry_threshold_bps: "50", exit_threshold_bps: "10", order_size: "0.01", trailing_stop_bps: "30", max_position: "1.0" } },
    { label: "Aggressive", params: { lookback_periods: 10, entry_threshold_bps: "30", exit_threshold_bps: "5", order_size: "0.02", trailing_stop_bps: "20", max_position: "2.0" } },
  ],
  mean_reversion: [
    { label: "Conservative", params: { window_periods: 30, num_std_dev: "2.5", order_size: "0.005", max_position: "0.5", exit_at_mean: true } },
    { label: "Moderate", params: { window_periods: 20, num_std_dev: "2.0", order_size: "0.01", max_position: "1.0", exit_at_mean: true } },
    { label: "Aggressive", params: { window_periods: 10, num_std_dev: "1.5", order_size: "0.02", max_position: "2.0", exit_at_mean: false } },
  ],
  stat_arb: [
    { label: "Conservative", params: { lookback_periods: 100, entry_z_score: "2.5", exit_z_score: "0.5", order_size: "0.005", max_position: "0.5" } },
    { label: "Moderate", params: { lookback_periods: 50, entry_z_score: "2.0", exit_z_score: "0.5", order_size: "0.01", max_position: "1.0" } },
    { label: "Aggressive", params: { lookback_periods: 20, entry_z_score: "1.5", exit_z_score: "0.3", order_size: "0.02", max_position: "2.0" } },
  ],
};
