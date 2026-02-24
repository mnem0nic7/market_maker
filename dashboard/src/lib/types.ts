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

export type ClientMessage =
  | { action: "subscribe"; channel: string }
  | { action: "unsubscribe"; channel: string }
  | { action: "ping" };
