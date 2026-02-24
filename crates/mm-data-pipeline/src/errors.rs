//! Crate-level error types for mm-data-pipeline.

use thiserror::Error;

/// Errors originating from the data-pipeline subsystem.
#[derive(Error, Debug)]
pub enum PipelineError {
    #[error("database error: {0}")]
    Database(String),

    #[error("messaging error: {0}")]
    Messaging(String),

    #[error("serialization error: {0}")]
    Serialization(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("{0}")]
    Internal(String),
}

impl From<PipelineError> for mm_core::MmError {
    fn from(e: PipelineError) -> Self {
        match e {
            PipelineError::Database(msg) => mm_core::MmError::Database(msg),
            PipelineError::Messaging(msg) => mm_core::MmError::Messaging(msg),
            PipelineError::Serialization(msg) => mm_core::MmError::Serialization(msg),
            PipelineError::Io(e) => mm_core::MmError::Internal(e.to_string()),
            PipelineError::Internal(msg) => mm_core::MmError::Internal(msg),
        }
    }
}

impl From<mm_core::MmError> for PipelineError {
    fn from(e: mm_core::MmError) -> Self {
        match e {
            mm_core::MmError::Database(msg) => PipelineError::Database(msg),
            mm_core::MmError::Messaging(msg) => PipelineError::Messaging(msg),
            mm_core::MmError::Serialization(msg) => PipelineError::Serialization(msg),
            other => PipelineError::Internal(other.to_string()),
        }
    }
}
