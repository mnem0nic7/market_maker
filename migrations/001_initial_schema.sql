-- Migration v1: Initial schema
-- Applied automatically by TimescaleStore::run_migrations()
-- This file is a reference copy of the DDL; the authoritative source is
-- crates/mm-data-pipeline/src/timescale.rs (apply_migration_v1).

-- Enable TimescaleDB
CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;

-- Schema version tracking
CREATE TABLE IF NOT EXISTS schema_migrations (
    version     INT         PRIMARY KEY,
    description TEXT        NOT NULL,
    applied_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Market data ticks (hypertable)
CREATE TABLE IF NOT EXISTS market_data_ticks (
    time         TIMESTAMPTZ NOT NULL,
    exchange     TEXT        NOT NULL,
    symbol       TEXT        NOT NULL,
    bid          NUMERIC     NOT NULL,
    ask          NUMERIC     NOT NULL,
    mid_price    NUMERIC     NOT NULL,
    spread_bps   NUMERIC     NOT NULL,
    volume       NUMERIC     NOT NULL DEFAULT 0
);
SELECT create_hypertable('market_data_ticks', 'time', if_not_exists => TRUE);

-- Trades (hypertable)
CREATE TABLE IF NOT EXISTS trades (
    time         TIMESTAMPTZ NOT NULL,
    exchange     TEXT        NOT NULL,
    symbol       TEXT        NOT NULL,
    side         TEXT        NOT NULL,
    price        NUMERIC     NOT NULL,
    quantity     NUMERIC     NOT NULL,
    trade_id     TEXT        NOT NULL
);
SELECT create_hypertable('trades', 'time', if_not_exists => TRUE);

-- Orders
CREATE TABLE IF NOT EXISTS orders (
    client_order_id    TEXT    PRIMARY KEY,
    exchange_order_id  TEXT,
    exchange           TEXT    NOT NULL,
    symbol             TEXT    NOT NULL,
    side               TEXT    NOT NULL,
    order_type         TEXT    NOT NULL,
    status             TEXT    NOT NULL,
    price              NUMERIC,
    quantity           NUMERIC NOT NULL,
    filled_quantity    NUMERIC NOT NULL DEFAULT 0,
    avg_fill_price     NUMERIC NOT NULL DEFAULT 0,
    strategy_id        TEXT    NOT NULL,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Fills (hypertable)
CREATE TABLE IF NOT EXISTS fills (
    time             TIMESTAMPTZ NOT NULL,
    fill_id          TEXT        NOT NULL,
    client_order_id  TEXT        NOT NULL,
    exchange         TEXT        NOT NULL,
    symbol           TEXT        NOT NULL,
    side             TEXT        NOT NULL,
    price            NUMERIC     NOT NULL,
    quantity         NUMERIC     NOT NULL,
    fee              NUMERIC     NOT NULL DEFAULT 0,
    fee_currency     TEXT        NOT NULL DEFAULT ''
);
SELECT create_hypertable('fills', 'time', if_not_exists => TRUE);

-- Positions
CREATE TABLE IF NOT EXISTS positions (
    exchange           TEXT    NOT NULL,
    symbol             TEXT    NOT NULL,
    quantity           NUMERIC NOT NULL DEFAULT 0,
    avg_entry_price    NUMERIC NOT NULL DEFAULT 0,
    unrealized_pnl     NUMERIC NOT NULL DEFAULT 0,
    realized_pnl       NUMERIC NOT NULL DEFAULT 0,
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (exchange, symbol)
);

-- Events (hypertable)
CREATE TABLE IF NOT EXISTS events (
    time            TIMESTAMPTZ NOT NULL,
    event_id        TEXT        NOT NULL,
    correlation_id  TEXT        NOT NULL,
    source          TEXT        NOT NULL,
    event_type      TEXT        NOT NULL,
    payload         JSONB       NOT NULL
);
SELECT create_hypertable('events', 'time', if_not_exists => TRUE);

-- Indices
CREATE INDEX IF NOT EXISTS idx_orders_strategy ON orders (strategy_id);
CREATE INDEX IF NOT EXISTS idx_events_type ON events (event_type, time DESC);
CREATE INDEX IF NOT EXISTS idx_fills_order ON fills (client_order_id, time DESC);

-- Record this migration
INSERT INTO schema_migrations (version, description) VALUES (1, 'Initial schema');
