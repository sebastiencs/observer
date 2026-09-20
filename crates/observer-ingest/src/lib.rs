//! Telemetry admission and ingestion services.

mod accept;
mod auth;
mod http;
mod logs;

pub use auth::{AuthConfigError, AuthError, TokenDirectory};
pub use http::LogsHttpService;
pub use logs::LogsIngestService;
pub use observer_protocol::{AcceptedBatch, AppendError, AppendErrorKind, IngestSink, Signal};
