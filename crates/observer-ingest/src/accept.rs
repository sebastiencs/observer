use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use observer_protocol::{
    AcceptedBatch, AppendError, IngestSink, Signal,
    otlp::{ExportLogsServiceRequest, ExportLogsServiceResponse},
};
use prost::Message;

/// Transport-independent failure from preparing or appending a logs batch.
#[derive(Debug)]
pub(crate) enum IngestLogsError {
    Clock(&'static str),
    Append(AppendError),
}

/// Decode → validate → canonical Protobuf → `AcceptedBatch` → sink.
pub(crate) async fn ingest_logs<S>(
    sink: &S,
    tenant_id: String,
    request: ExportLogsServiceRequest,
) -> Result<ExportLogsServiceResponse, IngestLogsError>
where
    S: IngestSink,
{
    let batch = prepare_logs_batch(tenant_id, request)?;
    sink.append(batch).await.map_err(IngestLogsError::Append)?;
    Ok(ExportLogsServiceResponse {
        partial_success: None,
    })
}

fn prepare_logs_batch(
    tenant_id: String,
    request: ExportLogsServiceRequest,
) -> Result<AcceptedBatch, IngestLogsError> {
    validate_logs_request(&request)?;
    Ok(AcceptedBatch {
        tenant_id,
        signal: Signal::Logs,
        received_at_unix_nanos: receive_time_unix_nanos()?,
        payload: Bytes::from(request.encode_to_vec()),
    })
}

fn validate_logs_request(_request: &ExportLogsServiceRequest) -> Result<(), IngestLogsError> {
    Ok(())
}

fn receive_time_unix_nanos() -> Result<u64, IngestLogsError> {
    let received_at = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| IngestLogsError::Clock("system clock is before the Unix epoch"))?;
    u64::try_from(received_at.as_nanos())
        .map_err(|_| IngestLogsError::Clock("receive timestamp exceeds u64"))
}
