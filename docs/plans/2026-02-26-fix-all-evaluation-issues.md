# Fix All Evaluation Issues — Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Fix all 13 critical/high issues found in the codebase evaluation, covering the dead order-update channel, kill switch desync, gateway security, risk engine bugs, WebSocket error broadcasting, Binance credential logging, and PnL accumulation.

**Architecture:** Fixes are organized by crate/binary to minimize cross-cutting conflicts. Each task is self-contained and testable independently.

**Tech Stack:** Rust, Tokio, Axum, tower-http

---

### Task 1: Fix dead order-update channel (CRITICAL #1)

**Files:**
- Modify: `crates/mm-execution/src/router.rs`
- Modify: `bins/trading-core/src/main.rs`

**Problem:** `_order_update_tx` is dropped immediately at main.rs:152. The router's internal update loop feeds the OMS but never notifies the main loop, so `handle_order_update` is never called. Fills never reach strategies.

**Fix:** Add a forwarding sender to `OrderRouter`. When `connect_all` is called, the router's update loop sends each update to both the OMS and a forwarding channel back to the main loop.

**Changes to `router.rs`:**
- Add `forward_tx: Option<mpsc::Sender<mm_core::OrderUpdate>>` field
- Add `pub fn set_forward_channel(&mut self, tx: mpsc::Sender<mm_core::OrderUpdate>)` method
- In `run_update_loop`, after updating OMS, clone and send to forward channel

**Changes to `main.rs`:**
- Replace `let (_order_update_tx, mut order_update_rx)` with `let (order_update_tx, mut order_update_rx)`
- Call `order_router.set_forward_channel(order_update_tx)` before `connect_all`

---

### Task 2: Fix kill switch desync between REST API and AtomicBool (CRITICAL #4)

**Files:**
- Modify: `crates/mm-gateway/src/rest.rs`

**Problem:** REST activate/reset only toggles `Mutex<bool>` in ApiState, not the real `AtomicBool` KillSwitch. Hot path checks the AtomicBool, so REST and hot path are desynchronized.

**Fix:** Add `Arc<KillSwitch>` to `ApiState`. REST handlers call `kill_switch.activate()` / `kill_switch.reset()` directly.

**Changes to `rest.rs` ApiState:**
- Add field: `pub kill_switch: Option<Arc<mm_risk::KillSwitch>>`
- In `activate_kill_switch`: if `state.kill_switch` is Some, call `.activate(ManualActivation)`
- In `reset_kill_switch`: if `state.kill_switch` is Some, call `.reset()`

**Changes to `main.rs`:**
- When constructing ApiState, set `kill_switch: Some(Arc::clone(&kill_switch))`

---

### Task 3: Fix CORS — restrict to configurable origins (CRITICAL #2)

**Files:**
- Modify: `crates/mm-gateway/src/server.rs`

**Problem:** Wildcard CORS (`allow_origin(Any)`) on all endpoints.

**Fix:** Add `cors_origins: Vec<String>` to `GatewayConfig`. Default to `["http://127.0.0.1:3000", "http://localhost:3000"]`. Use `AllowOrigin::list()` instead of `Any`.

---

### Task 4: Fix `connect_all` silent failure (HIGH #5)

**Files:**
- Modify: `crates/mm-execution/src/router.rs`

**Problem:** `connect_all` captures `first_error` but always returns `Ok(())`.

**Fix:** After starting the update loop, return the first error if set.

---

### Task 5: Fix PnL limit zero-guard logic (HIGH #6)

**Files:**
- Modify: `crates/mm-risk/src/limits.rs`

**Problem:** `PnlLimitCheck::check_order` uses `state.pnl != Decimal::ZERO` to decide between internal state and portfolio snapshot. Zero is a valid PnL value.

**Fix:** Add `has_recorded: bool` flag to `DailyPnlState`. Set to `true` in `record_pnl`. Use that flag instead of the zero-value guard.

---

### Task 6: Fix RateLimitCheck not recording on Allow (HIGH #7)

**Files:**
- Modify: `crates/mm-risk/src/limits.rs`

**Problem:** `check_order` returns `Allow` without recording the timestamp. Callers never call `record_order`. Rate limit is non-functional.

**Fix:** Record the timestamp in `check_order` before returning `Allow`.

---

### Task 7: Fix WebSocket error broadcasting to all clients (HIGH #9)

**Files:**
- Modify: `crates/mm-gateway/src/ws.rs`

**Problem:** Parse errors are sent via `broadcaster.broadcast_tx.send(err)`, reaching all clients.

**Fix:** Change `handle_client_message` to return a `Result` with error message. Caller sends error only to the originating client's sink, not the global broadcaster.

---

### Task 8: Fix Binance signed URL logging (HIGH #10)

**Files:**
- Modify: `crates/mm-execution/src/binance.rs`

**Problem:** `debug!(url = %url, "Binance: submitting order")` logs the full signed URL with HMAC signature.

**Fix:** Log only the non-sensitive parts (method, symbol, side, quantity). Remove the URL from the log line.

---

### Task 9: Fix daily PnL accumulation bug (HIGH #13)

**Files:**
- Modify: `bins/trading-core/src/main.rs`

**Problem:** At line 1059, `*daily_pnl.write().await = realized` overwrites daily PnL with one position's `realized_pnl` instead of accumulating.

**Fix:** Compute the delta from the previous realized_pnl for that position and add it to daily_pnl.

---

### Task 10: Fix position limit blocking reduce-only orders (HIGH #11)

**Files:**
- Modify: `crates/mm-risk/src/limits.rs`

**Problem:** Position limit check doesn't consider `reduce_only` flag or that an order may reduce an existing position.

**Fix:** If `new_order.reduce_only` is true, skip position limit check. Also, if an order reduces the absolute position (moves toward zero), always allow it.

---

### Task 11: Fix audit hash chain silent corruption (HIGH #8)

**Files:**
- Modify: `crates/mm-data-pipeline/src/audit.rs`

**Problem:** On restart, if the last audit line has a corrupted hash, the chain continues silently from the bad value.

**Fix:** On startup, call `verify_audit_file` if the file exists and is non-empty. If verification fails, return an error instead of silently accepting.

---

### Task 12: Verify all fixes compile and tests pass

**Run:** `cargo check --workspace && cargo test --workspace`

---
