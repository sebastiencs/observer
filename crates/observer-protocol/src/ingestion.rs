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

/// An error returned before a batch reaches its durable acknowledgement point.
#[derive(Debug)]
pub struct AppendError {
    message: String,
}

impl AppendError {
    #[must_use]
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
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
