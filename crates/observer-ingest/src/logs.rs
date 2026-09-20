use std::{
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use bytes::Bytes;
use observer_protocol::otlp::{
    ExportLogsServiceRequest, ExportLogsServiceResponse, LogsService, LogsServiceServer,
};
use prost::Message;
use tonic::{Request, Response, Status};

use observer_protocol::{AcceptedBatch, AppendErrorKind, IngestSink, Signal};

/// OTLP/gRPC logs ingestion service.
#[derive(Debug)]
pub struct LogsIngestService<S> {
    sink: Arc<S>,
    tenant_id: String,
}

impl<S> LogsIngestService<S> {
    #[must_use]
    pub fn new(sink: Arc<S>, tenant_id: impl Into<String>) -> Self {
        Self {
            sink,
            tenant_id: tenant_id.into(),
        }
    }
}

impl<S> LogsIngestService<S>
where
    S: IngestSink,
{
    #[must_use]
    pub fn into_server(self, max_decoding_message_size: usize) -> LogsServiceServer<Self> {
        LogsServiceServer::new(self).max_decoding_message_size(max_decoding_message_size)
    }
}

#[tonic::async_trait]
impl<S> LogsService for LogsIngestService<S>
where
    S: IngestSink,
{
    async fn export(
        &self,
        request: Request<ExportLogsServiceRequest>,
    ) -> Result<Response<ExportLogsServiceResponse>, Status> {
        let received_at = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| Status::internal("system clock is before the Unix epoch"))?;

        let payload = Bytes::from(request.into_inner().encode_to_vec());
        let batch = AcceptedBatch {
            tenant_id: self.tenant_id.clone(),
            signal: Signal::Logs,
            received_at_unix_nanos: u64::try_from(received_at.as_nanos())
                .map_err(|_| Status::internal("receive timestamp exceeds u64"))?,
            payload,
        };

        self.sink
            .append(batch)
            .await
            .map_err(|error| match error.kind() {
                AppendErrorKind::InvalidArgument => Status::invalid_argument(error.to_string()),
                AppendErrorKind::ResourceExhausted => Status::resource_exhausted(error.to_string()),
                AppendErrorKind::Unavailable => Status::unavailable(error.to_string()),
                AppendErrorKind::Internal => Status::internal(error.to_string()),
            })?;

        Ok(Response::new(ExportLogsServiceResponse {
            partial_success: None,
        }))
    }
}
