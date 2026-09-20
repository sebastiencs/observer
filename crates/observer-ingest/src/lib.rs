//! Telemetry admission and ingestion services.

mod logs;
mod sink;

pub use logs::LogsIngestService;
pub use sink::{AcceptedBatch, AppendError, IngestSink, Signal};
