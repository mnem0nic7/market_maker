//! # mm-gateway
//!
//! REST API, WebSocket broadcaster, and AI assistant for the market maker dashboard.
//!
//! - [`rest`] -- Axum REST API endpoints under `/api/v1/` with OpenAPI documentation
//! - [`ws`] -- WebSocket broadcaster for real-time dashboard updates
//! - [`assistant`] -- Claude AI chat assistant with SSE streaming
//! - [`grpc`] -- gRPC service definitions (planned)
//! - [`server`] -- [`GatewayServer`] wiring and graceful shutdown

pub mod assistant;
pub mod grpc;
pub mod research;
pub mod rest;
pub mod server;
pub mod ws;

pub use assistant::{AssistantConfig, ChatMessage, ChatRequest};
pub use rest::{ApiDoc, ApiState, RateLimitConfig, RateLimiter};
pub use server::{GatewayConfig, GatewayServer};
pub use ws::DashboardBroadcaster;
