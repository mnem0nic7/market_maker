//! Tamper-evident audit trail.
//!
//! [`AuditLogger`] consumes events from NATS JetStream and writes every event
//! to both the TimescaleDB `events` hypertable and a local append-only JSON
//! lines file. Each line is hash-chained using SHA-256 so that any retroactive
//! modification of the file can be detected.
//!
//! # File format (one JSON object per line)
//!
//! ```json
//! {"seq":1,"hash":"ab12...","envelope":{...}}
//! ```
//!
//! where `hash = hex(SHA256(previous_hash || envelope_json))`.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use chrono::Utc;
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::fs::OpenOptions;
use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tracing::{debug, error, info, instrument, warn};

use crate::event_store::NatsEventSubscriber;
use crate::timescale::TimescaleStore;

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/// A single line in the audit log file.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditLine {
    /// Monotonically increasing sequence number within this file.
    pub seq: u64,
    /// `hex(SHA256(previous_hash_bytes || envelope_json_bytes))`.
    pub hash: String,
    /// The original event envelope (verbatim).
    pub envelope: serde_json::Value,
}

/// Statistics tracked by the audit logger.
#[derive(Debug, Default)]
pub struct AuditStats {
    /// Total events successfully processed (file + DB).
    pub events_processed: AtomicU64,
    /// Total errors encountered (non-fatal; logged and skipped).
    pub errors: AtomicU64,
}

impl AuditStats {
    pub fn events_processed(&self) -> u64 {
        self.events_processed.load(Ordering::Relaxed)
    }

    pub fn errors(&self) -> u64 {
        self.errors.load(Ordering::Relaxed)
    }
}

// ---------------------------------------------------------------------------
// AuditLogger
// ---------------------------------------------------------------------------

/// Orchestrates the consumption of events and their dual-write to the
/// TimescaleDB events table and a local JSONL audit file.
pub struct AuditLogger {
    stats: Arc<AuditStats>,
    handle: Option<JoinHandle<()>>,
}

impl AuditLogger {
    /// Spawn the audit logger as a background tokio task.
    ///
    /// * `nats_subscriber` -- a connected [`NatsEventSubscriber`].
    /// * `timescale_store` -- a connected [`TimescaleStore`].
    /// * `audit_file_path` -- path to the append-only JSONL file (created if
    ///   missing).
    ///
    /// Returns `Self` which exposes live statistics and a handle to the
    /// background task.
    #[instrument(skip_all)]
    pub async fn start(
        nats_subscriber: NatsEventSubscriber,
        timescale_store: TimescaleStore,
        audit_file_path: impl AsRef<Path>,
    ) -> Result<Self, crate::errors::PipelineError> {
        let path = audit_file_path.as_ref().to_path_buf();
        tracing::Span::current().record("path", tracing::field::display(path.display()));
        let stats = Arc::new(AuditStats::default());
        let stats_inner = Arc::clone(&stats);

        // Determine the last hash from the existing file so we can continue
        // the chain.
        let last_hash = Self::read_last_hash(&path).await;
        let sequence_start = Self::read_last_sequence(&path).await;

        info!(
            sequence_start,
            chain_continues = last_hash.is_some(),
            "audit logger starting"
        );

        let handle = tokio::spawn(async move {
            if let Err(e) = Self::run_loop(
                nats_subscriber,
                timescale_store,
                path,
                last_hash,
                sequence_start,
                stats_inner,
            )
            .await
            {
                error!(error = %e, "audit logger terminated with error");
            }
        });

        Ok(Self {
            stats,
            handle: Some(handle),
        })
    }

    /// Access live statistics.
    pub fn stats(&self) -> &Arc<AuditStats> {
        &self.stats
    }

    /// Consume the logger and await the background task (blocks until the NATS
    /// stream ends or the task is cancelled).
    pub async fn shutdown(mut self) {
        if let Some(handle) = self.handle.take() {
            handle.abort();
            let _ = handle.await;
        }
        info!(
            events = self.stats.events_processed(),
            errors = self.stats.errors(),
            "audit logger shut down"
        );
    }

    // -----------------------------------------------------------------------
    // Internal
    // -----------------------------------------------------------------------

    /// Main event loop: subscribe to `events.>` and dual-write.
    async fn run_loop(
        subscriber: NatsEventSubscriber,
        store: TimescaleStore,
        path: PathBuf,
        initial_hash: Option<Vec<u8>>,
        start_seq: u64,
        stats: Arc<AuditStats>,
    ) -> Result<(), crate::errors::PipelineError> {
        let event_stream = subscriber.subscribe("events.>").await?;
        // The unfold-based stream is `!Unpin`, so we pin it on the heap.
        let mut event_stream = Box::pin(event_stream);

        let file = Arc::new(Mutex::new(
            OpenOptions::new()
                .create(true)
                .append(true)
                .open(&path)
                .await?,
        ));

        let mut prev_hash = initial_hash.unwrap_or_else(|| vec![0u8; 32]);
        let mut seq = start_seq;

        while let Some(envelope) = event_stream.next().await {
            seq += 1;
            let received_at = Utc::now();
            let lag_ms = (received_at - envelope.timestamp).num_milliseconds();

            // Serialize envelope to JSON.
            let envelope_json = match serde_json::to_value(&envelope) {
                Ok(v) => v,
                Err(e) => {
                    warn!(error = %e, "failed to serialize envelope, skipping");
                    stats.errors.fetch_add(1, Ordering::Relaxed);
                    continue;
                }
            };

            let envelope_bytes = match serde_json::to_vec(&envelope_json) {
                Ok(b) => b,
                Err(e) => {
                    warn!(error = %e, "failed to serialize envelope bytes, skipping");
                    stats.errors.fetch_add(1, Ordering::Relaxed);
                    continue;
                }
            };

            // Compute chained hash: SHA256(prev_hash || envelope_json_bytes).
            let current_hash = {
                let mut hasher = Sha256::new();
                hasher.update(&prev_hash);
                hasher.update(&envelope_bytes);
                hasher.finalize().to_vec()
            };
            let hash_hex = hex::encode(&current_hash);

            let audit_line = AuditLine {
                seq,
                hash: hash_hex,
                envelope: envelope_json,
            };

            // Write to file.
            let line = match serde_json::to_string(&audit_line) {
                Ok(mut s) => {
                    s.push('\n');
                    s
                }
                Err(e) => {
                    warn!(error = %e, seq, "failed to serialize audit line");
                    stats.errors.fetch_add(1, Ordering::Relaxed);
                    continue;
                }
            };

            {
                let mut f = file.lock().await;
                if let Err(e) = f.write_all(line.as_bytes()).await {
                    error!(error = %e, seq, "failed to write audit line to file");
                    stats.errors.fetch_add(1, Ordering::Relaxed);
                    // Continue to try DB write.
                }
                // Flush to ensure durability.
                if let Err(e) = f.flush().await {
                    warn!(error = %e, seq, "flush failed");
                }
            }

            // Write to TimescaleDB.
            if let Err(e) = store.persist_envelope(&envelope).await {
                error!(
                    error = %e,
                    event_id = %envelope.event_id,
                    "failed to persist envelope to TimescaleDB"
                );
                stats.errors.fetch_add(1, Ordering::Relaxed);
                // Non-fatal: the file write succeeded so we have an audit
                // record.
            }

            prev_hash = current_hash;
            stats.events_processed.fetch_add(1, Ordering::Relaxed);

            if seq % 1000 == 0 {
                info!(
                    seq,
                    events = stats.events_processed(),
                    errors = stats.errors(),
                    lag_ms,
                    "audit logger checkpoint"
                );
            } else {
                debug!(seq, lag_ms, event_id = %envelope.event_id, "event audited");
            }
        }

        info!(seq, "event stream ended, audit logger stopping");
        Ok(())
    }

    /// Read the last hash from the existing audit file, if any.
    async fn read_last_hash(path: &Path) -> Option<Vec<u8>> {
        let contents = tokio::fs::read_to_string(path).await.ok()?;
        let last_line = contents.lines().rev().find(|l| !l.trim().is_empty())?;
        let audit_line: AuditLine = serde_json::from_str(last_line).ok()?;
        hex::decode(&audit_line.hash).ok()
    }

    /// Read the last sequence number from the existing audit file, if any.
    async fn read_last_sequence(path: &Path) -> u64 {
        let contents = match tokio::fs::read_to_string(path).await {
            Ok(c) => c,
            Err(_) => return 0,
        };
        contents
            .lines()
            .rev()
            .find(|l| !l.trim().is_empty())
            .and_then(|l| serde_json::from_str::<AuditLine>(l).ok())
            .map(|a| a.seq)
            .unwrap_or(0)
    }
}

// ---------------------------------------------------------------------------
// Verification helper
// ---------------------------------------------------------------------------

/// Verify the integrity of an audit file by replaying the hash chain.
///
/// Returns `Ok(n)` with the number of verified lines, or an error describing
/// the first line where the chain breaks.
pub async fn verify_audit_file(path: impl AsRef<Path>) -> Result<u64, String> {
    let contents = tokio::fs::read_to_string(path.as_ref())
        .await
        .map_err(|e| format!("failed to read audit file: {e}"))?;

    let mut prev_hash: Vec<u8> = vec![0u8; 32];
    let mut count: u64 = 0;

    for (line_no, line) in contents.lines().enumerate() {
        if line.trim().is_empty() {
            continue;
        }

        let audit_line: AuditLine =
            serde_json::from_str(line).map_err(|e| format!("line {}: parse error: {e}", line_no + 1))?;

        let envelope_bytes = serde_json::to_vec(&audit_line.envelope)
            .map_err(|e| format!("line {}: envelope re-serialization failed: {e}", line_no + 1))?;

        let expected_hash = {
            let mut hasher = Sha256::new();
            hasher.update(&prev_hash);
            hasher.update(&envelope_bytes);
            hex::encode(hasher.finalize())
        };

        if audit_line.hash != expected_hash {
            return Err(format!(
                "line {}: hash mismatch (expected {}, got {})",
                line_no + 1,
                expected_hash,
                audit_line.hash
            ));
        }

        prev_hash = hex::decode(&audit_line.hash)
            .map_err(|e| format!("line {}: bad hex hash: {e}", line_no + 1))?;
        count += 1;
    }

    Ok(count)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hash_chain_deterministic() {
        let prev = vec![0u8; 32];
        let data = b"test-event-payload";

        let mut hasher = Sha256::new();
        hasher.update(&prev);
        hasher.update(data);
        let h1 = hex::encode(hasher.finalize());

        let mut hasher2 = Sha256::new();
        hasher2.update(&prev);
        hasher2.update(data);
        let h2 = hex::encode(hasher2.finalize());

        assert_eq!(h1, h2);
        assert_eq!(h1.len(), 64); // 32 bytes -> 64 hex chars
    }

    #[test]
    fn audit_line_serialization() {
        let line = AuditLine {
            seq: 1,
            hash: "aabbcc".to_string(),
            envelope: serde_json::json!({"type": "test"}),
        };
        let json = serde_json::to_string(&line).unwrap();
        let back: AuditLine = serde_json::from_str(&json).unwrap();
        assert_eq!(back.seq, 1);
        assert_eq!(back.hash, "aabbcc");
    }
}
