//! Telemetry admission and ingestion services.

mod logs;

pub use logs::LogsIngestService;
pub use observer_protocol::{AcceptedBatch, AppendError, AppendErrorKind, IngestSink, Signal};
