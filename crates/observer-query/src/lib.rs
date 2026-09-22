//! Internal SQL execution over one tenant snapshot.
//!
//! [`QueryEngine`] owns the shared DataFusion runtime. Later phases pin one store snapshot, register
//! a query-local `logs` table, and stream Arrow results. This crate does not expose an HTTP or gRPC
//! query API.

use std::sync::Arc;

use datafusion::execution::runtime_env::{RuntimeEnv, RuntimeEnvBuilder};

/// Shared DataFusion resources for tenant-scoped SQL execution.
pub struct QueryEngine {
    runtime: Arc<RuntimeEnv>,
}

/// Why the query runtime could not be constructed.
#[derive(Debug)]
pub enum QueryError {
    /// The DataFusion runtime could not be built.
    Resources(String),
}

impl std::fmt::Display for QueryError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Resources(detail) => write!(formatter, "query runtime: {detail}"),
        }
    }
}

impl std::error::Error for QueryError {}

impl QueryEngine {
    /// Build a runtime with DataFusion's default memory pool.
    ///
    /// # Errors
    ///
    /// Returns [`QueryError::Resources`] when the runtime cannot be constructed.
    pub fn new() -> Result<Self, QueryError> {
        let runtime = RuntimeEnvBuilder::new()
            .build()
            .map_err(|error| QueryError::Resources(error.to_string()))?;
        Ok(Self {
            runtime: Arc::new(runtime),
        })
    }

    /// Shared execution resources for every query-local session.
    #[must_use]
    pub fn runtime(&self) -> &RuntimeEnv {
        &self.runtime
    }
}

#[cfg(test)]
mod tests {
    use super::QueryEngine;

    #[test]
    fn builds_a_shared_runtime() {
        let engine = QueryEngine::new().expect("runtime");
        assert_eq!(engine.runtime().memory_pool.reserved(), 0);
    }
}
