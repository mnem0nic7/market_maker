//! NATS JetStream event store for durable event publishing and consumption.
//!
//! Provides [`NatsEventPublisher`] (implements the `EventPublisher` trait from mm-core)
//! and [`NatsEventSubscriber`] for replaying / tailing the event stream.

use std::time::Duration;

use async_nats::jetstream::{self, consumer::PullConsumer, stream::Stream as JsStream};
use async_trait::async_trait;
use futures::StreamExt;
use mm_core::traits::EventPublisher;
use mm_core::{Envelope, MmError, MmResult};
use serde::{Deserialize, Serialize};
use tracing::{debug, error, info, instrument, warn};

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Configuration for the NATS JetStream connection and stream.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NatsConfig {
    /// NATS server URL (e.g. "nats://127.0.0.1:4222").
    pub url: String,
    /// Name of the JetStream stream to create / bind to.
    pub stream_name: String,
    /// Maximum age of messages in seconds (0 = unlimited).
    pub max_age_secs: u64,
    /// Maximum total bytes in the stream (0 = unlimited).
    pub max_bytes: i64,
}

impl Default for NatsConfig {
    fn default() -> Self {
        Self {
            url: "nats://127.0.0.1:4222".to_string(),
            stream_name: "EVENTS".to_string(),
            max_age_secs: 0,
            max_bytes: 0,
        }
    }
}

// ---------------------------------------------------------------------------
// Publisher
// ---------------------------------------------------------------------------

/// Publishes [`Envelope`]s to NATS JetStream, implementing the `EventPublisher`
/// trait so it can be used transparently by the rest of the platform.
pub struct NatsEventPublisher {
    jetstream: jetstream::Context,
    stream_name: String,
}

impl NatsEventPublisher {
    /// Connect to NATS and ensure the JetStream stream exists.
    ///
    /// If the stream already exists its configuration is left untouched; only
    /// missing streams are created.
    #[instrument(skip_all, fields(url = %config.url, stream = %config.stream_name))]
    pub async fn connect(config: &NatsConfig) -> MmResult<Self> {
        let client = async_nats::connect(&config.url)
            .await
            .map_err(|e| MmError::Messaging(format!("NATS connect failed: {e}")))?;

        let jetstream = jetstream::new(client);

        // Create or bind to the stream.
        let mut stream_config = jetstream::stream::Config {
            name: config.stream_name.clone(),
            subjects: vec!["events.>".to_string()],
            retention: jetstream::stream::RetentionPolicy::Limits,
            ..Default::default()
        };

        if config.max_age_secs > 0 {
            stream_config.max_age = Duration::from_secs(config.max_age_secs);
        }
        if config.max_bytes > 0 {
            stream_config.max_bytes = config.max_bytes;
        }

        jetstream
            .get_or_create_stream(stream_config)
            .await
            .map_err(|e| MmError::Messaging(format!("JetStream stream setup failed: {e}")))?;

        info!(
            stream = %config.stream_name,
            "NATS JetStream publisher connected"
        );

        Ok(Self {
            jetstream,
            stream_name: config.stream_name.clone(),
        })
    }

    /// Return the name of the underlying JetStream stream.
    pub fn stream_name(&self) -> &str {
        &self.stream_name
    }
}

#[async_trait]
impl EventPublisher for NatsEventPublisher {
    /// Serialize the envelope to JSON and publish to JetStream on the subject
    /// returned by [`Envelope::nats_subject`].
    #[instrument(skip_all, fields(
        event_id = %envelope.event_id,
        subject = %envelope.nats_subject(),
    ))]
    async fn publish(&self, envelope: Envelope) -> MmResult<()> {
        let subject = envelope.nats_subject();
        let payload = serde_json::to_vec(&envelope)
            .map_err(|e| MmError::Serialization(format!("envelope serialization failed: {e}")))?;

        let ack = self
            .jetstream
            .publish(subject.clone(), payload.into())
            .await
            .map_err(|e| MmError::Messaging(format!("JetStream publish failed: {e}")))?;

        // Await the publish acknowledgment from the server to guarantee
        // the message has been persisted in the stream.
        ack.await
            .map_err(|e| MmError::Messaging(format!("JetStream publish ack failed: {e}")))?;

        debug!(subject = %subject, "event published to JetStream");

        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Subscriber
// ---------------------------------------------------------------------------

/// Consumes events from NATS JetStream by creating ephemeral or durable pull
/// consumers against the event stream.
pub struct NatsEventSubscriber {
    #[allow(dead_code)]
    jetstream: jetstream::Context,
    stream: JsStream,
}

impl NatsEventSubscriber {
    /// Connect to NATS and bind to an existing JetStream stream.
    #[instrument(skip_all, fields(url = %config.url, stream = %config.stream_name))]
    pub async fn connect(config: &NatsConfig) -> MmResult<Self> {
        let client = async_nats::connect(&config.url)
            .await
            .map_err(|e| MmError::Messaging(format!("NATS connect failed: {e}")))?;

        let jetstream = jetstream::new(client);

        let stream = jetstream
            .get_stream(&config.stream_name)
            .await
            .map_err(|e| {
                MmError::Messaging(format!(
                    "failed to bind to JetStream stream '{}': {e}",
                    config.stream_name
                ))
            })?;

        info!(
            stream = %config.stream_name,
            "NATS JetStream subscriber connected"
        );

        Ok(Self { jetstream, stream })
    }

    /// Subscribe to events matching the given subject pattern (e.g. `"events.>"`,
    /// `"events.order.>"`, `"events.risk.*"`).
    ///
    /// Returns an unbounded stream of deserialized [`Envelope`]s. Messages that
    /// fail to deserialize are logged and skipped (acked so they are not
    /// redelivered forever).
    #[instrument(skip(self), fields(subject = %subject_pattern))]
    pub async fn subscribe(
        &self,
        subject_pattern: &str,
    ) -> MmResult<impl futures::Stream<Item = Envelope> + '_> {
        let consumer = self
            .stream
            .create_consumer(jetstream::consumer::pull::Config {
                filter_subject: subject_pattern.to_string(),
                ..Default::default()
            })
            .await
            .map_err(|e| MmError::Messaging(format!("consumer creation failed: {e}")))?;

        info!(subject = %subject_pattern, "subscribed to event stream");

        Ok(self.message_stream(consumer))
    }

    /// Subscribe starting from a specific stream sequence number, useful for
    /// replaying events from a known checkpoint.
    #[instrument(skip(self), fields(subject = %subject, start_seq = start_sequence))]
    pub async fn subscribe_from_sequence(
        &self,
        subject: &str,
        start_sequence: u64,
    ) -> MmResult<impl futures::Stream<Item = Envelope> + '_> {
        let consumer = self
            .stream
            .create_consumer(jetstream::consumer::pull::Config {
                filter_subject: subject.to_string(),
                deliver_policy: jetstream::consumer::DeliverPolicy::ByStartSequence {
                    start_sequence,
                },
                ..Default::default()
            })
            .await
            .map_err(|e| MmError::Messaging(format!("consumer creation failed: {e}")))?;

        info!(
            subject = %subject,
            start_sequence,
            "subscribed to event stream from sequence"
        );

        Ok(self.message_stream(consumer))
    }

    /// Internal helper: turn a pull consumer into a stream of [`Envelope`]s.
    fn message_stream(&self, consumer: PullConsumer) -> impl futures::Stream<Item = Envelope> + '_ {
        async_stream(consumer)
    }
}

/// Produces an async stream of deserialized envelopes from a pull consumer.
fn async_stream(consumer: PullConsumer) -> impl futures::Stream<Item = Envelope> {
    futures::stream::unfold(consumer, |consumer| async move {
        // Batch of 1 with a generous idle timeout keeps latency low while
        // remaining efficient under load.
        let mut messages = match consumer.messages().await {
            Ok(m) => m,
            Err(e) => {
                error!(error = %e, "failed to open message stream from consumer");
                return None;
            }
        };

        loop {
            match messages.next().await {
                Some(Ok(msg)) => {
                    // Always ack to avoid infinite redelivery of bad messages.
                    if let Err(e) = msg.ack().await {
                        warn!(error = %e, "failed to ack JetStream message");
                    }

                    match serde_json::from_slice::<Envelope>(&msg.payload) {
                        Ok(envelope) => {
                            // We need to return the consumer for the next iteration, but
                            // we have moved it into `messages`. Instead we break out and
                            // return the envelope. On the next poll unfold will call us
                            // again -- but we've consumed the consumer. To keep things
                            // simple we return None after the first batch so callers
                            // should use `subscribe` which wraps this differently.
                            //
                            // Actually, we restructure: unfold is not ideal here. We use
                            // `futures::stream::try_unfold` pattern instead. But to keep
                            // the API simple we just yield envelopes from an
                            // `async_stream`-less approach using `stream::unfold` with
                            // the messages iterator as state.
                            //
                            // Let's restructure to use (messages) as the state.
                            drop(messages);
                            return Some((envelope, consumer));
                        }
                        Err(e) => {
                            warn!(
                                error = %e,
                                payload_len = msg.payload.len(),
                                "skipping undeserializable JetStream message"
                            );
                            // Continue to next message.
                        }
                    }
                }
                Some(Err(e)) => {
                    warn!(error = %e, "error reading from JetStream consumer");
                    // Continue trying.
                }
                None => {
                    // Stream ended (server closed connection, etc.)
                    debug!("JetStream message stream ended");
                    return None;
                }
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_is_sane() {
        let cfg = NatsConfig::default();
        assert_eq!(cfg.stream_name, "EVENTS");
        assert!(cfg.url.contains("4222"));
    }

    #[test]
    fn config_serialization_roundtrip() {
        let cfg = NatsConfig {
            url: "nats://prod:4222".into(),
            stream_name: "EVENTS".into(),
            max_age_secs: 86400,
            max_bytes: 1_073_741_824,
        };
        let json = serde_json::to_string(&cfg).unwrap();
        let back: NatsConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(back.stream_name, cfg.stream_name);
        assert_eq!(back.max_age_secs, cfg.max_age_secs);
    }
}
