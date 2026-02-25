"use client";

import {
  createContext, useContext, useReducer, useEffect, useRef, useCallback,
  type ReactNode, type Dispatch,
} from "react";
import { api } from "./api";
import { WebSocketManager } from "./websocket";
import type {
  Position, Order, PortfolioRisk, StrategyStatus, ServerMessage, MarketDataTickMsg,
  StrategyMetrics,
} from "./types";

export interface DashboardState {
  connected: boolean;
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
  | { type: "SET_CONNECTED"; connected: boolean }
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
      return { ...state, connected: action.connected };
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

  const handleStatus = useCallback((connected: boolean) => {
    dispatch({ type: "SET_CONNECTED", connected });
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
