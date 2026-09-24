use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use observer_protocol::{
    AcceptedBatch, AppendError, IngestSink, Signal,
    otlp::{ExportLogsServiceRequest, ExportLogsServiceResponse},
};
use prost::Message;

/// Stable rejection when a client event timestamp is too far ahead of receive time.
pub const FUTURE_TIMESTAMP: &str = "event timestamp is too far in the future";

/// How far ahead of receive time a client event timestamp may be.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct IngestOptions {
    /// Maximum `effective_event_time - received_at`.
    pub max_future_skew: Duration,
    /// When set, use this receive time instead of the system clock.
    pub receive_time_unix_nano: Option<u64>,
}

impl Default for IngestOptions {
    fn default() -> Self {
        Self {
            max_future_skew: Duration::from_millis(86_400_000),
            receive_time_unix_nano: None,
        }
    }
}

/// Transport-independent failure from preparing or appending a logs batch.
#[derive(Debug)]
pub(crate) enum IngestLogsError {
    Clock(&'static str),
    FutureTimestamp,
    Append(AppendError),
}

/// Decode → validate → canonical Protobuf → `AcceptedBatch` → sink.
pub(crate) async fn ingest_logs<S>(
    sink: &S,
    tenant_id: String,
    request: ExportLogsServiceRequest,
    options: IngestOptions,
) -> Result<ExportLogsServiceResponse, IngestLogsError>
where
    S: IngestSink,
{
    let batch = prepare_logs_batch(tenant_id, request, options)?;
    sink.append(batch).await.map_err(IngestLogsError::Append)?;
    Ok(ExportLogsServiceResponse {
        partial_success: None,
    })
}

fn prepare_logs_batch(
    tenant_id: String,
    request: ExportLogsServiceRequest,
    options: IngestOptions,
) -> Result<AcceptedBatch, IngestLogsError> {
    let received_at_unix_nanos = match options.receive_time_unix_nano {
        Some(received) => received,
        None => receive_time_unix_nanos()?,
    };
    validate_logs_request(&request, received_at_unix_nanos, &options)?;
    Ok(AcceptedBatch {
        tenant_id,
        signal: Signal::Logs,
        received_at_unix_nanos,
        payload: Bytes::from(request.encode_to_vec()),
    })
}

fn validate_logs_request(
    request: &ExportLogsServiceRequest,
    received_at_unix_nanos: u64,
    options: &IngestOptions,
) -> Result<(), IngestLogsError> {
    let skew = u64::try_from(options.max_future_skew.as_nanos()).unwrap_or(u64::MAX);
    let limit = received_at_unix_nanos.saturating_add(skew);
    for resource_logs in &request.resource_logs {
        for scope_logs in &resource_logs.scope_logs {
            for record in &scope_logs.log_records {
                let effective = present_time(record.time_unix_nano)
                    .or_else(|| present_time(record.observed_time_unix_nano))
                    .unwrap_or(received_at_unix_nanos);
                if effective > limit {
                    return Err(IngestLogsError::FutureTimestamp);
                }
            }
        }
    }
    Ok(())
}

fn present_time(unix_nano: u64) -> Option<u64> {
    (unix_nano != 0).then_some(unix_nano)
}

fn receive_time_unix_nanos() -> Result<u64, IngestLogsError> {
    let received_at = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| IngestLogsError::Clock("system clock is before the Unix epoch"))?;
    u64::try_from(received_at.as_nanos())
        .map_err(|_| IngestLogsError::Clock("receive timestamp exceeds u64"))
}
