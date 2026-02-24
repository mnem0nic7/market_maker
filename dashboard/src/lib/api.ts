import type {
  ApiResponse, HealthResponse, Position, Order, PortfolioRisk,
  StrategyStatus, OrderBook, KillSwitchResponse,
} from "./types";

const BASE = "/api/v1";

async function fetchApi<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(`${BASE}${path}`, {
    headers: { "Content-Type": "application/json", ...init?.headers },
    ...init,
  });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`API ${res.status}: ${text}`);
  }
  const envelope: ApiResponse<T> = await res.json();
  if (envelope.status === "error") {
    throw new Error(envelope.error ?? "Unknown API error");
  }
  return envelope.data as T;
}

export const api = {
  health: () => fetchApi<HealthResponse>("/health"),
  positions: () => fetchApi<Position[]>("/positions"),
  orders: (params?: { exchange?: string; symbol?: string; strategy_id?: string }) => {
    const query = new URLSearchParams();
    if (params?.exchange) query.set("exchange", params.exchange);
    if (params?.symbol) query.set("symbol", params.symbol);
    if (params?.strategy_id) query.set("strategy_id", params.strategy_id);
    const qs = query.toString();
    return fetchApi<Order[]>(`/orders${qs ? `?${qs}` : ""}`);
  },
  order: (id: string) => fetchApi<Order>(`/orders/${encodeURIComponent(id)}`),
  portfolioRisk: () => fetchApi<PortfolioRisk>("/portfolio/risk"),
  strategies: () => fetchApi<StrategyStatus[]>("/strategies"),
  startStrategy: (id: string) =>
    fetchApi<string>(`/strategies/${encodeURIComponent(id)}/start`, { method: "POST" }),
  stopStrategy: (id: string) =>
    fetchApi<string>(`/strategies/${encodeURIComponent(id)}/stop`, { method: "POST" }),
  activateKillSwitch: () =>
    fetchApi<KillSwitchResponse>("/risk/kill-switch", { method: "POST" }),
  resetKillSwitch: () =>
    fetchApi<KillSwitchResponse>("/risk/kill-switch", { method: "DELETE" }),
  orderbook: (exchange: string, symbol: string) =>
    fetchApi<OrderBook>(`/market-data/${encodeURIComponent(exchange)}/${encodeURIComponent(symbol)}/orderbook`),
};
