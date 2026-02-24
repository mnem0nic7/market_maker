//! TimescaleDB persistence layer.
//!
//! [`TimescaleStore`] wraps a `sqlx::PgPool` and provides typed insert/upsert
//! and query methods for market data, trades, orders, fills, positions, and
//! events. Time-series tables are created as TimescaleDB hypertables for
//! efficient range queries and automatic chunk management.

use chrono::{DateTime, Utc};
use mm_core::Envelope;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use tracing::{info, instrument};

use crate::errors::PipelineError;

// ---------------------------------------------------------------------------
// Store
// ---------------------------------------------------------------------------

/// Wraps a PostgreSQL / TimescaleDB connection pool and exposes typed
/// persistence operations for all domain tables.
#[derive(Clone)]
pub struct TimescaleStore {
    pool: PgPool,
}

impl TimescaleStore {
    /// Create a new store by connecting to the database at `database_url`.
    ///
    /// The pool is configured with sensible defaults for a trading workload
    /// (moderate concurrency, short idle timeout).
    #[instrument(skip_all)]
    pub async fn connect(database_url: &str) -> Result<Self, PipelineError> {
        let pool = PgPoolOptions::new()
            .max_connections(20)
            .acquire_timeout(std::time::Duration::from_secs(5))
            .idle_timeout(std::time::Duration::from_secs(300))
            .connect(database_url)
            .await
            .map_err(|e| PipelineError::Database(format!("connection failed: {e}")))?;

        info!("connected to TimescaleDB");

        Ok(Self { pool })
    }

    /// Return a reference to the underlying connection pool.
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    // -----------------------------------------------------------------------
    // Migrations
    // -----------------------------------------------------------------------

    /// Create all required tables and hypertables if they do not already exist.
    ///
    /// This is intentionally idempotent so it can be called on every startup.
    #[instrument(skip(self))]
    pub async fn run_migrations(&self) -> Result<(), PipelineError> {
        // Ensure the TimescaleDB extension is available.
        sqlx::query("CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE")
            .execute(&self.pool)
            .await
            .map_err(|e| PipelineError::Database(format!("timescaledb extension: {e}")))?;

        // -- market_data_ticks --
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS market_data_ticks (
                time         TIMESTAMPTZ NOT NULL,
                exchange     TEXT        NOT NULL,
                symbol       TEXT        NOT NULL,
                bid          NUMERIC     NOT NULL,
                ask          NUMERIC     NOT NULL,
                mid_price    NUMERIC     NOT NULL,
                spread_bps   NUMERIC     NOT NULL,
                volume       NUMERIC     NOT NULL DEFAULT 0
            )
            "#,
        )
        .execute(&self.pool)
        .await
        .map_err(|e| PipelineError::Database(format!("market_data_ticks table: {e}")))?;

        Self::create_hypertable(&self.pool, "market_data_ticks", "time").await?;

        // -- trades --
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS trades (
                time         TIMESTAMPTZ NOT NULL,
                exchange     TEXT        NOT NULL,
                symbol       TEXT        NOT NULL,
                side         TEXT        NOT NULL,
                price        NUMERIC     NOT NULL,
                quantity     NUMERIC     NOT NULL,
                trade_id     TEXT        NOT NULL
            )
            "#,
        )
        .execute(&self.pool)
        .await
        .map_err(|e| PipelineError::Database(format!("trades table: {e}")))?;

        Self::create_hypertable(&self.pool, "trades", "time").await?;

        // -- orders --
        sqlx::query(
            r#"
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
            )
            "#,
        )
        .execute(&self.pool)
        .await
        .map_err(|e| PipelineError::Database(format!("orders table: {e}")))?;

        // -- fills --
        sqlx::query(
            r#"
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
            )
            "#,
        )
        .execute(&self.pool)
        .await
        .map_err(|e| PipelineError::Database(format!("fills table: {e}")))?;

        Self::create_hypertable(&self.pool, "fills", "time").await?;

        // -- positions --
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS positions (
                exchange           TEXT    NOT NULL,
                symbol             TEXT    NOT NULL,
                quantity           NUMERIC NOT NULL DEFAULT 0,
                avg_entry_price    NUMERIC NOT NULL DEFAULT 0,
                unrealized_pnl     NUMERIC NOT NULL DEFAULT 0,
                realized_pnl       NUMERIC NOT NULL DEFAULT 0,
                updated_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                PRIMARY KEY (exchange, symbol)
            )
            "#,
        )
        .execute(&self.pool)
        .await
        .map_err(|e| PipelineError::Database(format!("positions table: {e}")))?;

        // -- events --
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS events (
                time            TIMESTAMPTZ NOT NULL,
                event_id        TEXT        NOT NULL,
                correlation_id  TEXT        NOT NULL,
                source          TEXT        NOT NULL,
                event_type      TEXT        NOT NULL,
                payload         JSONB       NOT NULL
            )
            "#,
        )
        .execute(&self.pool)
        .await
        .map_err(|e| PipelineError::Database(format!("events table: {e}")))?;

        Self::create_hypertable(&self.pool, "events", "time").await?;

        // -- useful indices --
        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_orders_strategy ON orders (strategy_id)",
        )
        .execute(&self.pool)
        .await
        .map_err(|e| PipelineError::Database(format!("orders index: {e}")))?;

        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_events_type ON events (event_type, time DESC)",
        )
        .execute(&self.pool)
        .await
        .map_err(|e| PipelineError::Database(format!("events index: {e}")))?;

        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_fills_order ON fills (client_order_id, time DESC)",
        )
        .execute(&self.pool)
        .await
        .map_err(|e| PipelineError::Database(format!("fills index: {e}")))?;

        info!("TimescaleDB migrations complete");
        Ok(())
    }

    /// Helper: create a hypertable, silently succeeding if it already exists.
    async fn create_hypertable(
        pool: &PgPool,
        table: &str,
        time_column: &str,
    ) -> Result<(), PipelineError> {
        let sql = format!(
            "SELECT create_hypertable('{table}', '{time_column}', if_not_exists => TRUE)"
        );
        sqlx::query(&sql)
            .execute(pool)
            .await
            .map_err(|e| PipelineError::Database(format!("create_hypertable({table}): {e}")))?;
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Insert helpers
    // -----------------------------------------------------------------------

    /// Insert a market data tick.
    #[instrument(skip(self))]
    pub async fn insert_market_tick(
        &self,
        time: DateTime<Utc>,
        exchange: &str,
        symbol: &str,
        bid: Decimal,
        ask: Decimal,
        mid_price: Decimal,
        spread_bps: Decimal,
        volume: Decimal,
    ) -> Result<(), PipelineError> {
        sqlx::query(
            r#"
            INSERT INTO market_data_ticks (time, exchange, symbol, bid, ask, mid_price, spread_bps, volume)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            "#,
        )
        .bind(time)
        .bind(exchange)
        .bind(symbol)
        .bind(bid)
        .bind(ask)
        .bind(mid_price)
        .bind(spread_bps)
        .bind(volume)
        .execute(&self.pool)
        .await
        .map_err(|e| PipelineError::Database(format!("insert_market_tick: {e}")))?;

        Ok(())
    }

    /// Insert a trade record.
    #[instrument(skip(self))]
    pub async fn insert_trade(
        &self,
        time: DateTime<Utc>,
        exchange: &str,
        symbol: &str,
        side: &str,
        price: Decimal,
        quantity: Decimal,
        trade_id: &str,
    ) -> Result<(), PipelineError> {
        sqlx::query(
            r#"
            INSERT INTO trades (time, exchange, symbol, side, price, quantity, trade_id)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            "#,
        )
        .bind(time)
        .bind(exchange)
        .bind(symbol)
        .bind(side)
        .bind(price)
        .bind(quantity)
        .bind(trade_id)
        .execute(&self.pool)
        .await
        .map_err(|e| PipelineError::Database(format!("insert_trade: {e}")))?;

        Ok(())
    }

    /// Upsert an order (insert or update on conflict with client_order_id).
    #[instrument(skip(self))]
    pub async fn upsert_order(
        &self,
        client_order_id: &str,
        exchange_order_id: Option<&str>,
        exchange: &str,
        symbol: &str,
        side: &str,
        order_type: &str,
        status: &str,
        price: Option<Decimal>,
        quantity: Decimal,
        filled_quantity: Decimal,
        avg_fill_price: Decimal,
        strategy_id: &str,
        created_at: DateTime<Utc>,
        updated_at: DateTime<Utc>,
    ) -> Result<(), PipelineError> {
        sqlx::query(
            r#"
            INSERT INTO orders (
                client_order_id, exchange_order_id, exchange, symbol, side,
                order_type, status, price, quantity, filled_quantity,
                avg_fill_price, strategy_id, created_at, updated_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
            ON CONFLICT (client_order_id) DO UPDATE SET
                exchange_order_id = COALESCE(EXCLUDED.exchange_order_id, orders.exchange_order_id),
                status            = EXCLUDED.status,
                filled_quantity   = EXCLUDED.filled_quantity,
                avg_fill_price    = EXCLUDED.avg_fill_price,
                updated_at        = EXCLUDED.updated_at
            "#,
        )
        .bind(client_order_id)
        .bind(exchange_order_id)
        .bind(exchange)
        .bind(symbol)
        .bind(side)
        .bind(order_type)
        .bind(status)
        .bind(price)
        .bind(quantity)
        .bind(filled_quantity)
        .bind(avg_fill_price)
        .bind(strategy_id)
        .bind(created_at)
        .bind(updated_at)
        .execute(&self.pool)
        .await
        .map_err(|e| PipelineError::Database(format!("upsert_order: {e}")))?;

        Ok(())
    }

    /// Insert a fill record.
    #[instrument(skip(self))]
    pub async fn insert_fill(
        &self,
        time: DateTime<Utc>,
        fill_id: &str,
        client_order_id: &str,
        exchange: &str,
        symbol: &str,
        side: &str,
        price: Decimal,
        quantity: Decimal,
        fee: Decimal,
        fee_currency: &str,
    ) -> Result<(), PipelineError> {
        sqlx::query(
            r#"
            INSERT INTO fills (time, fill_id, client_order_id, exchange, symbol, side, price, quantity, fee, fee_currency)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            "#,
        )
        .bind(time)
        .bind(fill_id)
        .bind(client_order_id)
        .bind(exchange)
        .bind(symbol)
        .bind(side)
        .bind(price)
        .bind(quantity)
        .bind(fee)
        .bind(fee_currency)
        .execute(&self.pool)
        .await
        .map_err(|e| PipelineError::Database(format!("insert_fill: {e}")))?;

        Ok(())
    }

    /// Upsert a position (insert or update on conflict with exchange + symbol).
    #[instrument(skip(self))]
    pub async fn upsert_position(
        &self,
        exchange: &str,
        symbol: &str,
        quantity: Decimal,
        avg_entry_price: Decimal,
        unrealized_pnl: Decimal,
        realized_pnl: Decimal,
        updated_at: DateTime<Utc>,
    ) -> Result<(), PipelineError> {
        sqlx::query(
            r#"
            INSERT INTO positions (exchange, symbol, quantity, avg_entry_price, unrealized_pnl, realized_pnl, updated_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            ON CONFLICT (exchange, symbol) DO UPDATE SET
                quantity        = EXCLUDED.quantity,
                avg_entry_price = EXCLUDED.avg_entry_price,
                unrealized_pnl  = EXCLUDED.unrealized_pnl,
                realized_pnl    = EXCLUDED.realized_pnl,
                updated_at      = EXCLUDED.updated_at
            "#,
        )
        .bind(exchange)
        .bind(symbol)
        .bind(quantity)
        .bind(avg_entry_price)
        .bind(unrealized_pnl)
        .bind(realized_pnl)
        .bind(updated_at)
        .execute(&self.pool)
        .await
        .map_err(|e| PipelineError::Database(format!("upsert_position: {e}")))?;

        Ok(())
    }

    /// Insert a generic event into the events hypertable.
    #[instrument(skip(self, payload))]
    pub async fn insert_event(
        &self,
        time: DateTime<Utc>,
        event_id: &str,
        correlation_id: &str,
        source: &str,
        event_type: &str,
        payload: &serde_json::Value,
    ) -> Result<(), PipelineError> {
        sqlx::query(
            r#"
            INSERT INTO events (time, event_id, correlation_id, source, event_type, payload)
            VALUES ($1, $2, $3, $4, $5, $6)
            "#,
        )
        .bind(time)
        .bind(event_id)
        .bind(correlation_id)
        .bind(source)
        .bind(event_type)
        .bind(payload)
        .execute(&self.pool)
        .await
        .map_err(|e| PipelineError::Database(format!("insert_event: {e}")))?;

        Ok(())
    }

    // -----------------------------------------------------------------------
    // Query helpers
    // -----------------------------------------------------------------------

    /// Retrieve trades for a given symbol within a time range.
    #[instrument(skip(self))]
    pub async fn get_trades(
        &self,
        symbol: &str,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
    ) -> Result<Vec<TradeRow>, PipelineError> {
        let rows = sqlx::query_as::<_, TradeRow>(
            r#"
            SELECT time, exchange, symbol, side, price, quantity, trade_id
            FROM trades
            WHERE symbol = $1 AND time >= $2 AND time < $3
            ORDER BY time ASC
            "#,
        )
        .bind(symbol)
        .bind(from)
        .bind(to)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| PipelineError::Database(format!("get_trades: {e}")))?;

        Ok(rows)
    }

    /// Retrieve all orders for a given strategy.
    #[instrument(skip(self))]
    pub async fn get_orders_by_strategy(
        &self,
        strategy_id: &str,
    ) -> Result<Vec<OrderRow>, PipelineError> {
        let rows = sqlx::query_as::<_, OrderRow>(
            r#"
            SELECT client_order_id, exchange_order_id, exchange, symbol, side,
                   order_type, status, price, quantity, filled_quantity,
                   avg_fill_price, strategy_id, created_at, updated_at
            FROM orders
            WHERE strategy_id = $1
            ORDER BY created_at DESC
            "#,
        )
        .bind(strategy_id)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| PipelineError::Database(format!("get_orders_by_strategy: {e}")))?;

        Ok(rows)
    }

    /// Retrieve the current position for a given exchange and symbol.
    #[instrument(skip(self))]
    pub async fn get_position(
        &self,
        exchange: &str,
        symbol: &str,
    ) -> Result<Option<PositionRow>, PipelineError> {
        let row = sqlx::query_as::<_, PositionRow>(
            r#"
            SELECT exchange, symbol, quantity, avg_entry_price, unrealized_pnl, realized_pnl, updated_at
            FROM positions
            WHERE exchange = $1 AND symbol = $2
            "#,
        )
        .bind(exchange)
        .bind(symbol)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| PipelineError::Database(format!("get_position: {e}")))?;

        Ok(row)
    }

    /// Retrieve events within a time range, optionally filtered by event_type.
    #[instrument(skip(self))]
    pub async fn get_events(
        &self,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
        event_type_filter: Option<&str>,
    ) -> Result<Vec<EventRow>, PipelineError> {
        let rows = match event_type_filter {
            Some(event_type) => {
                sqlx::query_as::<_, EventRow>(
                    r#"
                    SELECT time, event_id, correlation_id, source, event_type, payload
                    FROM events
                    WHERE time >= $1 AND time < $2 AND event_type = $3
                    ORDER BY time ASC
                    "#,
                )
                .bind(from)
                .bind(to)
                .bind(event_type)
                .fetch_all(&self.pool)
                .await
            }
            None => {
                sqlx::query_as::<_, EventRow>(
                    r#"
                    SELECT time, event_id, correlation_id, source, event_type, payload
                    FROM events
                    WHERE time >= $1 AND time < $2
                    ORDER BY time ASC
                    "#,
                )
                .bind(from)
                .bind(to)
                .fetch_all(&self.pool)
                .await
            }
        }
        .map_err(|e| PipelineError::Database(format!("get_events: {e}")))?;

        Ok(rows)
    }

    /// Higher-level helper: persist a full [`Envelope`] to the events table by
    /// extracting the event type tag from the serialized payload.
    pub async fn persist_envelope(&self, envelope: &Envelope) -> Result<(), PipelineError> {
        let payload = serde_json::to_value(&envelope.payload)
            .map_err(|e| PipelineError::Serialization(format!("envelope payload: {e}")))?;

        let event_type = payload
            .get("type")
            .and_then(|v| v.as_str())
            .unwrap_or("Unknown")
            .to_string();

        self.insert_event(
            envelope.timestamp,
            &envelope.event_id,
            &envelope.correlation_id,
            &envelope.source,
            &event_type,
            &payload,
        )
        .await
    }
}

// ---------------------------------------------------------------------------
// Row types (for sqlx::FromRow)
// ---------------------------------------------------------------------------

/// A row from the `trades` hypertable.
#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct TradeRow {
    pub time: DateTime<Utc>,
    pub exchange: String,
    pub symbol: String,
    pub side: String,
    pub price: Decimal,
    pub quantity: Decimal,
    pub trade_id: String,
}

/// A row from the `orders` table.
#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct OrderRow {
    pub client_order_id: String,
    pub exchange_order_id: Option<String>,
    pub exchange: String,
    pub symbol: String,
    pub side: String,
    pub order_type: String,
    pub status: String,
    pub price: Option<Decimal>,
    pub quantity: Decimal,
    pub filled_quantity: Decimal,
    pub avg_fill_price: Decimal,
    pub strategy_id: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// A row from the `positions` table.
#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct PositionRow {
    pub exchange: String,
    pub symbol: String,
    pub quantity: Decimal,
    pub avg_entry_price: Decimal,
    pub unrealized_pnl: Decimal,
    pub realized_pnl: Decimal,
    pub updated_at: DateTime<Utc>,
}

/// A row from the `events` hypertable.
#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct EventRow {
    pub time: DateTime<Utc>,
    pub event_id: String,
    pub correlation_id: String,
    pub source: String,
    pub event_type: String,
    pub payload: serde_json::Value,
}

/// A row from the `fills` hypertable.
#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct FillRow {
    pub time: DateTime<Utc>,
    pub fill_id: String,
    pub client_order_id: String,
    pub exchange: String,
    pub symbol: String,
    pub side: String,
    pub price: Decimal,
    pub quantity: Decimal,
    pub fee: Decimal,
    pub fee_currency: String,
}

/// A row from the `market_data_ticks` hypertable.
#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct MarketTickRow {
    pub time: DateTime<Utc>,
    pub exchange: String,
    pub symbol: String,
    pub bid: Decimal,
    pub ask: Decimal,
    pub mid_price: Decimal,
    pub spread_bps: Decimal,
    pub volume: Decimal,
}
