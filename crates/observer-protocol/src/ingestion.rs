use std::{error::Error, fmt};

use bytes::Bytes;

/// The telemetry signal contained in an accepted batch.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Signal {
    Logs,
}

/// A validated request ready for durable persistence.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AcceptedBatch {
    pub tenant_id: String,
    pub signal: Signal,
    pub received_at_unix_nanos: u64,
    pub payload: Bytes,
}

/// Why an ingest append failed, used to map transport status codes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AppendErrorKind {
    InvalidArgument,
    ResourceExhausted,
    Unavailable,
    Internal,
}

/// An error returned before a batch reaches its durable acknowledgement point.
#[derive(Clone, Debug)]
pub struct AppendError {
    kind: AppendErrorKind,
    message: String,
}

impl AppendError {
    #[must_use]
    pub fn new(message: impl Into<String>) -> Self {
        Self::unavailable(message)
    }

    #[must_use]
    pub fn invalid_argument(message: impl Into<String>) -> Self {
        Self {
            kind: AppendErrorKind::InvalidArgument,
            message: message.into(),
        }
    }

    #[must_use]
    pub fn resource_exhausted(message: impl Into<String>) -> Self {
        Self {
            kind: AppendErrorKind::ResourceExhausted,
            message: message.into(),
        }
    }

    #[must_use]
    pub fn unavailable(message: impl Into<String>) -> Self {
        Self {
            kind: AppendErrorKind::Unavailable,
            message: message.into(),
        }
    }

    #[must_use]
    pub fn internal(message: impl Into<String>) -> Self {
        Self {
            kind: AppendErrorKind::Internal,
            message: message.into(),
        }
    }

    #[must_use]
    pub fn kind(&self) -> AppendErrorKind {
        self.kind
    }
}

impl fmt::Display for AppendError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl Error for AppendError {}

/// Destination for accepted telemetry batches.
///
/// A successful result means the batch has crossed the sink's durable
/// acknowledgement boundary. The WAL implementation will satisfy this contract
/// only after `fsync` completes.
#[tonic::async_trait]
pub trait IngestSink: Send + Sync + 'static {
    async fn append(&self, batch: AcceptedBatch) -> Result<(), AppendError>;
}
