"use client";

import {
  createContext, useContext, useReducer, useEffect, useRef, useCallback,
  type ReactNode, type Dispatch,
} from "react";
import { api } from "./api";
import { WebSocketManager, type ConnectionStatus } from "./websocket";
import type {
  Position, Order, PortfolioRisk, StrategyStatus, ServerMessage, MarketDataTickMsg,
  StrategyMetrics,
} from "./types";

export interface DashboardState {
  connected: boolean;
  connectionStatus: ConnectionStatus;
  positions: Position[];
  orders: Order[];
  risk: PortfolioRisk | null;
  strategies: StrategyStatus[];
  metrics: Record<string, StrategyMetrics>;
  ticks: Record<string, MarketDataTickMsg>;
  events: ServerMessage[];
  loading: boolean;
  error: string | null;
}

const initialState: DashboardState = {
  connected: false,
  connectionStatus: "disconnected",
  positions: [],
  orders: [],
  risk: null,
  strategies: [],
  metrics: {},
  ticks: {},
  events: [],
  loading: true,
  error: null,
};

export type Action =
  | { type: "SET_CONNECTED"; status: ConnectionStatus }
  | { type: "SET_POSITIONS"; positions: Position[] }
  | { type: "SET_ORDERS"; orders: Order[] }
  | { type: "SET_RISK"; risk: PortfolioRisk }
  | { type: "SET_STRATEGIES"; strategies: StrategyStatus[] }
  | { type: "WS_MESSAGE"; msg: ServerMessage }
  | { type: "SET_LOADING"; loading: boolean }
  | { type: "SET_ERROR"; error: string | null };

const MAX_EVENTS = 200;

function tickKey(exchange: string, symbol: { base: string; quote: string }): string {
  return `${exchange}:${symbol.base}/${symbol.quote}`;
}

function reducer(state: DashboardState, action: Action): DashboardState {
  switch (action.type) {
    case "SET_CONNECTED":
      return { ...state, connected: action.status === "connected", connectionStatus: action.status };
    case "SET_POSITIONS":
      return { ...state, positions: action.positions };
    case "SET_ORDERS":
      return { ...state, orders: action.orders };
    case "SET_RISK":
      return { ...state, risk: action.risk };
    case "SET_STRATEGIES":
      return { ...state, strategies: action.strategies };
    case "SET_LOADING":
      return { ...state, loading: action.loading };
    case "SET_ERROR":
      return { ...state, error: action.error };
    case "WS_MESSAGE": {
      const msg = action.msg;
      const events = [msg, ...state.events].slice(0, MAX_EVENTS);
      switch (msg.type) {
        case "MarketDataTick":
          return {
            ...state,
            events,
            ticks: { ...state.ticks, [tickKey(msg.exchange, msg.symbol)]: msg },
          };
        case "RiskUpdate":
          return {
            ...state,
            events,
            risk: state.risk
              ? { ...state.risk, net_exposure: msg.net_exposure, gross_exposure: msg.gross_exposure, daily_pnl: msg.daily_pnl }
              : null,
          };
        case "StrategyMetrics":
          return {
            ...state,
            metrics: {
              ...state.metrics,
              [msg.strategy_id]: msg.metrics,
            },
            events,
          };
        case "PositionUpdate": {
          const posIdx = state.positions.findIndex(
            (p) => p.exchange === msg.exchange && p.symbol.base === msg.symbol.base && p.symbol.quote === msg.symbol.quote,
          );
          let updatedPositions: Position[];
          if (posIdx >= 0) {
            updatedPositions = state.positions.map((p, i) =>
              i === posIdx
                ? { ...p, quantity: msg.quantity, unrealized_pnl: msg.unrealized_pnl, realized_pnl: msg.realized_pnl, updated_at: new Date().toISOString() }
                : p,
            );
          } else {
            updatedPositions = [
              ...state.positions,
              {
                exchange: msg.exchange,
                symbol: msg.symbol,
                quantity: msg.quantity,
                average_entry_price: "0",
                unrealized_pnl: msg.unrealized_pnl,
                realized_pnl: msg.realized_pnl,
                updated_at: new Date().toISOString(),
              },
            ];
          }
          return { ...state, positions: updatedPositions, events };
        }
        case "OrderUpdate": {
          const TERMINAL_STATUSES = ["Filled", "Cancelled", "Rejected", "Expired"];
          const isTerminal = TERMINAL_STATUSES.includes(msg.status);

          if (isTerminal) {
            // Remove terminal orders from the active list.
            return {
              ...state,
              orders: state.orders.filter((o) => o.client_order_id !== msg.client_order_id),
              events,
            };
          }

          const ordIdx = state.orders.findIndex((o) => o.client_order_id === msg.client_order_id);
          let updatedOrders: Order[];
          if (ordIdx >= 0) {
            updatedOrders = state.orders.map((o, i) =>
              i === ordIdx
                ? { ...o, status: msg.status as Order["status"], filled_quantity: msg.filled_quantity, remaining_quantity: msg.remaining_quantity, updated_at: new Date().toISOString() }
                : o,
            );
          } else {
            updatedOrders = [
              ...state.orders,
              {
                client_order_id: msg.client_order_id,
                exchange_order_id: null,
                exchange: msg.exchange,
                symbol: msg.symbol,
                side: "Buy" as const,
                order_type: "Limit" as const,
                time_in_force: "GTC" as const,
                price: null,
                quantity: msg.remaining_quantity,
                status: msg.status as Order["status"],
                filled_quantity: msg.filled_quantity,
                remaining_quantity: msg.remaining_quantity,
                average_fill_price: "0",
                strategy_id: "",
                reduce_only: false,
                created_at: new Date().toISOString(),
                updated_at: new Date().toISOString(),
              },
            ];
          }
          return { ...state, orders: updatedOrders, events };
        }
        default:
          return { ...state, events };
      }
    }
    default:
      return state;
  }
}

const StateContext = createContext<DashboardState>(initialState);
const DispatchContext = createContext<Dispatch<Action>>(() => {});

export function useDashboard() {
  return useContext(StateContext);
}

export function useDashboardDispatch() {
  return useContext(DispatchContext);
}

export function DashboardProvider({ children }: { children: ReactNode }) {
  const [state, dispatch] = useReducer(reducer, initialState);
  const wsRef = useRef<WebSocketManager | null>(null);

  const handleMessage = useCallback((msg: ServerMessage) => {
    dispatch({ type: "WS_MESSAGE", msg });
  }, []);

  const handleStatus = useCallback((status: ConnectionStatus) => {
    dispatch({ type: "SET_CONNECTED", status });
  }, []);

  useEffect(() => {
    async function hydrate() {
      try {
        const [positions, orders, strategies] = await Promise.allSettled([
          api.positions(), api.orders(), api.strategies(),
        ]);
        if (positions.status === "fulfilled") dispatch({ type: "SET_POSITIONS", positions: positions.value });
        if (orders.status === "fulfilled") dispatch({ type: "SET_ORDERS", orders: orders.value });
        if (strategies.status === "fulfilled") dispatch({ type: "SET_STRATEGIES", strategies: strategies.value });
        try {
          const risk = await api.portfolioRisk();
          dispatch({ type: "SET_RISK", risk });
        } catch { /* Risk may not be populated */ }
      } catch (e) {
        dispatch({ type: "SET_ERROR", error: String(e) });
      } finally {
        dispatch({ type: "SET_LOADING", loading: false });
      }
    }
    hydrate();
  }, []);

  useEffect(() => {
    const protocol = window.location.protocol === "https:" ? "wss:" : "ws:";
    const wsUrl = process.env.NODE_ENV === "development"
      ? "ws://localhost:8080/api/v1/ws"
      : `${protocol}//${window.location.host}/api/v1/ws`;
    const ws = new WebSocketManager(wsUrl, handleMessage, handleStatus);
    ws.connect();
    wsRef.current = ws;
    return () => { ws.disconnect(); };
  }, [handleMessage, handleStatus]);

  return (
    <StateContext.Provider value={state}>
      <DispatchContext.Provider value={dispatch}>{children}</DispatchContext.Provider>
    </StateContext.Provider>
  );
}
