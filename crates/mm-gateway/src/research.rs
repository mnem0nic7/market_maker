use std::sync::Arc;

use async_trait::async_trait;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::Json;
use futures::StreamExt;
use serde::{Deserialize, Serialize};

use mm_research::agent::AgentRunner;
use mm_research::llm::{AnthropicProvider, OpenAiProvider};
use mm_research::skills::SkillLoader;
use mm_research::tools::finance::{register_finance_tools, FinancialDataClient};
use mm_research::tools::registry::ToolRegistry;
use mm_research::tools::search::build_search_tool;
use mm_research::tools::trading::{register_trading_tools, TradingContext};

use crate::rest::{ApiResponse, ApiState};

// ---------------------------------------------------------------------------
// TradingContext implementation for ApiState
// ---------------------------------------------------------------------------

/// Wrapper to implement TradingContext for gateway state.
///
/// We cannot impl a foreign trait on `Arc<ApiState>` (orphan rule), so we wrap
/// the inner `Arc<ApiState>` in a newtype and implement `TradingContext` on it.
pub struct GatewayTradingContext(pub Arc<ApiState>);

#[async_trait]
impl TradingContext for GatewayTradingContext {
    async fn get_positions(&self) -> Vec<serde_json::Value> {
        let positions = self.0.positions.read().await;
        positions
            .iter()
            .map(|p| {
                serde_json::json!({
                    "exchange": p.exchange.to_string(),
                    "symbol": p.symbol.to_string(),
                    "quantity": p.quantity.to_string(),
                    "average_entry_price": p.average_entry_price.to_string(),
                    "unrealized_pnl": p.unrealized_pnl.to_string(),
                    "realized_pnl": p.realized_pnl.to_string(),
                })
            })
            .collect()
    }

    async fn get_order_books(&self) -> Vec<serde_json::Value> {
        let books = self.0.order_books.read().await;
        books
            .iter()
            .map(|((exchange, symbol), book)| {
                let best_bid = book.best_bid().map(|level| {
                    serde_json::json!({"price": level.price.to_string(), "quantity": level.quantity.to_string()})
                });
                let best_ask = book.best_ask().map(|level| {
                    serde_json::json!({"price": level.price.to_string(), "quantity": level.quantity.to_string()})
                });
                serde_json::json!({
                    "exchange": exchange.to_string(),
                    "symbol": symbol,
                    "best_bid": best_bid,
                    "best_ask": best_ask,
                })
            })
            .collect()
    }

    async fn get_strategies(&self) -> Vec<serde_json::Value> {
        let strategies = self.0.strategies.read().await;
        strategies
            .iter()
            .map(|s| {
                serde_json::json!({
                    "strategy_id": s.config.strategy_id,
                    "strategy_type": s.config.strategy_type.to_string(),
                    "exchange": s.config.exchange.to_string(),
                    "symbol": s.config.symbol.to_string(),
                    "running": s.running,
                })
            })
            .collect()
    }

    async fn get_risk_state(&self) -> serde_json::Value {
        let ks = self.0.kill_switch_active.lock().await;
        let risk = self.0.portfolio_risk.read().await;

        let mut state = serde_json::json!({
            "kill_switch_active": *ks,
        });

        if let Some(ref r) = *risk {
            state["net_exposure"] = serde_json::json!(r.net_exposure.to_string());
            state["gross_exposure"] = serde_json::json!(r.gross_exposure.to_string());
            state["daily_pnl"] = serde_json::json!(r.daily_pnl.to_string());
            state["max_drawdown"] = serde_json::json!(r.max_drawdown.to_string());
        }

        state
    }
}

// ---------------------------------------------------------------------------
// Request/response types
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
pub struct ResearchRequest {
    pub query: String,
    #[serde(default)]
    pub include_trading_context: bool,
}

#[derive(Debug, Serialize)]
pub struct ToolInfo {
    pub name: String,
    pub description: String,
}

#[derive(Debug, Serialize)]
pub struct SkillInfo {
    pub name: String,
    pub description: String,
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

/// POST /api/v1/research/query — run the research agent, returning SSE events.
pub async fn research_query(
    State(state): State<Arc<ApiState>>,
    Json(req): Json<ResearchRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ApiResponse<()>>)> {
    let config = match &state.research_config {
        Some(cfg) => (**cfg).clone(),
        None => {
            return Err(ApiResponse::error(
                "Research agent not configured. Set MM_RESEARCH__ANTHROPIC_API_KEY.",
            ));
        }
    };

    if !config.enabled {
        return Err(ApiResponse::error("Research agent is disabled."));
    }

    // Build LLM provider
    let llm: Arc<dyn mm_research::llm::LlmProvider> = match config.llm_provider.as_str() {
        "anthropic" => {
            let api_key = config.anthropic_api_key.clone().ok_or_else(|| {
                ApiResponse::error("Anthropic API key not configured.")
            })?;
            Arc::new(AnthropicProvider::new(api_key, config.llm_model.clone()))
        }
        "openai" => {
            let api_key = config.openai_api_key.clone().ok_or_else(|| {
                ApiResponse::error("OpenAI API key not configured.")
            })?;
            Arc::new(OpenAiProvider::new(api_key, config.llm_model.clone()))
        }
        other => {
            return Err(ApiResponse::error(format!(
                "Unsupported LLM provider: {other}"
            )));
        }
    };

    // Build tool registry
    let mut registry = ToolRegistry::new();

    if let Some(ref key) = config.financial_data_api_key {
        let client = Arc::new(FinancialDataClient::new(key.clone()));
        register_finance_tools(&mut registry, client);
    }

    if let Some(search_tool) = build_search_tool(&config) {
        registry.register(search_tool);
    }

    if req.include_trading_context {
        let ctx: Arc<dyn TradingContext> = Arc::new(GatewayTradingContext(state.clone()));
        register_trading_tools(&mut registry, ctx);
    }

    let tools = Arc::new(registry);

    // Load skills
    let skills = SkillLoader::load_from_dirs(&config.skill_dirs);

    // Build trading context string for system prompt
    let trading_ctx = if req.include_trading_context {
        let gateway_ctx = GatewayTradingContext(state.clone());
        let positions = gateway_ctx.get_positions().await;
        let strategies = gateway_ctx.get_strategies().await;
        let risk = gateway_ctx.get_risk_state().await;
        Some(format!(
            "Positions: {}\nStrategies: {}\nRisk: {}",
            serde_json::to_string(&positions).unwrap_or_default(),
            serde_json::to_string(&strategies).unwrap_or_default(),
            serde_json::to_string(&risk).unwrap_or_default(),
        ))
    } else {
        None
    };

    // Run agent
    let runner = AgentRunner::new(llm, tools, skills, config);
    let event_stream = runner.run(req.query, trading_ctx);

    // Map AgentEvents to SSE format
    let sse_stream = event_stream.map(|event| {
        Ok::<_, std::convert::Infallible>(format!(
            "data: {}\n\n",
            serde_json::to_string(&event).unwrap_or_default()
        ))
    });

    Ok((
        [(axum::http::header::CONTENT_TYPE, "text/event-stream")],
        axum::body::Body::from_stream(sse_stream),
    ))
}

/// GET /api/v1/research/tools — list available research tools.
pub async fn research_tools(
    State(state): State<Arc<ApiState>>,
) -> Json<ApiResponse<Vec<ToolInfo>>> {
    let config = state
        .research_config
        .as_ref()
        .map(|c| (**c).clone())
        .unwrap_or_default();

    let mut tools = Vec::new();

    if config.financial_data_api_key.is_some() {
        let client = Arc::new(FinancialDataClient::new(String::new()));
        let mut registry = ToolRegistry::new();
        register_finance_tools(&mut registry, client);
        for def in registry.as_tool_definitions() {
            tools.push(ToolInfo {
                name: def.name,
                description: def.description,
            });
        }
    }

    if build_search_tool(&config).is_some() {
        tools.push(ToolInfo {
            name: "web_search".into(),
            description: "Search the web for recent information".into(),
        });
    }

    // Trading tools are always available
    tools.push(ToolInfo {
        name: "get_trading_positions".into(),
        description: "Get current trading positions".into(),
    });
    tools.push(ToolInfo {
        name: "get_trading_order_books".into(),
        description: "Get current order book snapshots".into(),
    });
    tools.push(ToolInfo {
        name: "get_trading_strategies".into(),
        description: "Get active trading strategies".into(),
    });
    tools.push(ToolInfo {
        name: "get_trading_risk_state".into(),
        description: "Get current risk state".into(),
    });

    ApiResponse::ok(tools)
}

/// GET /api/v1/research/skills — list loaded skills.
pub async fn research_skills(
    State(state): State<Arc<ApiState>>,
) -> Json<ApiResponse<Vec<SkillInfo>>> {
    let config = state
        .research_config
        .as_ref()
        .map(|c| (**c).clone())
        .unwrap_or_default();

    let skills = SkillLoader::load_from_dirs(&config.skill_dirs);
    let info: Vec<SkillInfo> = skills
        .into_iter()
        .map(|s| SkillInfo {
            name: s.name,
            description: s.description,
        })
        .collect();

    ApiResponse::ok(info)
}
