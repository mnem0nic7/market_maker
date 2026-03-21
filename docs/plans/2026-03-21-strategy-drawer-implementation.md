# Strategy Drawer Implementation

## Goal

Replace the inline strategy expansion in the dashboard with a dedicated drawer that gives operators one place to understand a strategy, inspect upside and downside risk, review live metrics, edit settings, and understand the practical effect of starting or stopping the strategy.

## Scope

This implementation is split into two layers.

1. Ship a dashboard drawer now using the strategy metadata, live metrics, and existing REST endpoints already available in the product.
2. Follow with deeper backend work so the drawer can render true strategy-level risk snapshots and more explicit lifecycle guarantees.

The first layer is sufficient to improve operator usability immediately. The second layer is where the risk model becomes more rigorous.

## User Experience

When the user clicks a strategy card in the dashboard:

1. A right-side drawer opens.
2. The drawer shows:
   - strategy identity, venue, and run state
   - a plain-language explainer for how the strategy works
   - upside and downside risk summaries
   - live metrics and recent lifecycle timestamps
   - editable settings for known parameters
   - advanced raw parameters for full visibility
   - a backend-aware explanation of start, stop, and save behavior
3. The user can:
   - start or stop the strategy
   - edit supported parameters
   - apply updates without leaving the drawer
   - jump into optimization from the same surface

## Current-Phase Implementation

### Frontend

- Add a dedicated `StrategyDrawer` component in the dashboard.
- Refactor the strategies panel so each card opens the drawer instead of using inline expansion.
- Reuse the existing strategy metadata and `StrategyGuide` content to avoid duplicate product copy.
- Use the existing REST endpoints:
  - `GET /api/v1/strategies`
  - `POST /api/v1/strategies/{id}/start`
  - `POST /api/v1/strategies/{id}/stop`
  - `PATCH /api/v1/strategies/{id}/params`
  - `PUT /api/v1/strategies/{id}`

### Risk Presentation

The first implementation uses a practical summary built from:

- strategy type metadata
- known parameter values
- current live metrics
- running state

This does not pretend to be a full risk engine. It is an operator-oriented summary that explains:

- where the strategy is supposed to make money
- what market conditions hurt it
- whether current settings look conservative or aggressive
- whether current live metrics suggest churn, drag, or healthy execution

### Start/Stop Semantics Shown In The Drawer

The drawer copy must reflect current backend behavior:

- `START` validates the current config, builds the strategy instance, registers it with the engine, marks it running, and sets `started_at`.
- `STOP` removes the strategy from the engine, prevents new strategy decisions, and sets `stopped_at`.
- Outstanding exchange orders may still depend on downstream cancellation behavior; the drawer must describe stop as halting strategy logic, not as a guaranteed position flatten.
- `SAVE SETTINGS` hot-applies while the strategy is running because the gateway update paths push param changes through the runtime update channel.

## Follow-On Backend Work

The drawer is the new UX surface, but it still deserves a stronger backend contract. The next backend phase should add:

1. A dedicated strategy detail endpoint.
2. Shared strategy risk and lifecycle response types.
3. Strategy-level drawdown, exposure, and position metrics.
4. Explicit stop semantics for open strategy-owned orders.
5. Optional WebSocket updates for lifecycle and risk state.

## File Targets

- `dashboard/src/components/panels/Strategies.tsx`
- `dashboard/src/components/panels/StrategyDrawer.tsx`
- `dashboard/src/components/panels/StrategyGuide.tsx`
- `dashboard/src/lib/api.ts`
- `dashboard/src/lib/types.ts`
- `crates/mm-gateway/src/rest.rs`
- `crates/mm-core/src/strategy_metrics.rs`
- `bins/trading-core/src/main.rs`

## Acceptance Criteria

### Current phase

1. Clicking a strategy opens a drawer instead of expanding the card inline.
2. The drawer shows strategy explanation, risk framing, metrics, settings, and lifecycle timestamps.
3. The drawer can start and stop a strategy.
4. The drawer can edit supported parameters and save them.
5. The drawer explains what start, stop, and save do in system terms.

### Follow-on backend phase

1. Strategy risk data becomes measured rather than heuristic.
2. Strategy detail is served from a dedicated backend contract.
3. Stop semantics are explicit about open order handling.

## Verification

1. Run `npm run lint` in `dashboard`.
2. Run `npm run build` in `dashboard`.
3. Open the dashboard and verify:
   - strategy cards open the drawer
   - metrics continue to update
   - parameter saves work for stopped and running strategies
   - lifecycle timestamps update after start and stop
   - the optimization modal still opens from the strategy list and the drawer