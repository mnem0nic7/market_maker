pub mod avellaneda;
pub mod engine;
pub mod grid;
pub mod mean_reversion;
pub mod momentum;
pub mod spread;
pub mod stat_arb;
pub mod twap;
pub mod vwap;

pub use avellaneda::{AvellanedaStoikovConfig, AvellanedaStoikovStrategy};
pub use engine::{StrategyEngine, StrategyParamUpdate};
pub use grid::{GridConfig, GridStrategy, GridType};
pub use mean_reversion::{MeanReversionConfig, MeanReversionStrategy};
pub use momentum::{MomentumConfig, MomentumStrategy};
pub use spread::{SpreadConfig, SpreadStrategy};
pub use stat_arb::{StatArbConfig, StatArbStrategy};
pub use twap::{TwapConfig, TwapStrategy};
pub use vwap::{VwapConfig, VwapStrategy};

/// Build a boxed strategy instance from a [`mm_core::StrategyConfig`].
///
/// Injects `strategy_id`, `exchange`, and `symbol` into `params` before
/// deserializing so callers don't need to duplicate those fields.
pub fn build_strategy_from_config(
    config: &mm_core::StrategyConfig,
) -> Result<Box<dyn mm_core::traits::Strategy>, String> {
    let exchange = config.exchange;
    let symbol = config.symbol.clone();

    // Inject identity fields into params so the typed config structs can
    // deserialize regardless of whether the caller included them.
    let mut params = config.params.clone();
    if let Some(obj) = params.as_object_mut() {
        obj.entry("strategy_id")
            .or_insert_with(|| serde_json::json!(config.strategy_id));
        obj.entry("exchange")
            .or_insert_with(|| serde_json::json!(exchange));
        obj.entry("symbol")
            .or_insert_with(|| serde_json::json!(symbol));
    }

    match config.strategy_type.as_str() {
        "spread" => {
            let cfg: SpreadConfig = serde_json::from_value(params)
                .map_err(|e| format!("invalid spread params: {e}"))?;
            cfg.validate()?;
            Ok(Box::new(SpreadStrategy::new(cfg)))
        }
        "avellaneda" => {
            let cfg: AvellanedaStoikovConfig = serde_json::from_value(params)
                .map_err(|e| format!("invalid avellaneda params: {e}"))?;
            cfg.validate()?;
            Ok(Box::new(AvellanedaStoikovStrategy::new(cfg)))
        }
        "grid" => {
            let cfg: GridConfig =
                serde_json::from_value(params).map_err(|e| format!("invalid grid params: {e}"))?;
            cfg.validate()?;
            Ok(Box::new(GridStrategy::new(cfg)))
        }
        "twap" => {
            let cfg: TwapConfig =
                serde_json::from_value(params).map_err(|e| format!("invalid twap params: {e}"))?;
            cfg.validate()?;
            Ok(Box::new(TwapStrategy::new(cfg)))
        }
        "vwap" => {
            let cfg: VwapConfig =
                serde_json::from_value(params).map_err(|e| format!("invalid vwap params: {e}"))?;
            cfg.validate()?;
            Ok(Box::new(VwapStrategy::new(cfg)))
        }
        "momentum" => {
            let cfg: MomentumConfig = serde_json::from_value(params)
                .map_err(|e| format!("invalid momentum params: {e}"))?;
            cfg.validate()?;
            Ok(Box::new(MomentumStrategy::new(cfg)))
        }
        "mean_reversion" => {
            let cfg: MeanReversionConfig = serde_json::from_value(params)
                .map_err(|e| format!("invalid mean_reversion params: {e}"))?;
            cfg.validate()?;
            Ok(Box::new(MeanReversionStrategy::new(cfg)))
        }
        "stat_arb" => {
            let cfg: StatArbConfig = serde_json::from_value(params)
                .map_err(|e| format!("invalid stat_arb params: {e}"))?;
            cfg.validate()?;
            Ok(Box::new(StatArbStrategy::new(cfg)))
        }
        other => Err(format!("unsupported strategy type: {other}")),
    }
}
