//! Telemetry admission and ingestion services.

mod accept;
mod http;
mod logs;

pub use http::LogsHttpService;
pub use logs::LogsIngestService;
pub use observer_protocol::{AcceptedBatch, AppendError, AppendErrorKind, IngestSink, Signal};
