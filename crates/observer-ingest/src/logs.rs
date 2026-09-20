use std::sync::Arc;

use observer_protocol::{
    AppendErrorKind, IngestSink,
    otlp::{ExportLogsServiceRequest, ExportLogsServiceResponse, LogsService, LogsServiceServer},
};
use tonic::{Request, Response, Status};

use crate::accept::{IngestLogsError, ingest_logs};

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
        ingest_logs(&*self.sink, self.tenant_id.clone(), request.into_inner())
            .await
            .map(Response::new)
            .map_err(status_from_ingest)
    }
}

fn status_from_ingest(error: IngestLogsError) -> Status {
    match error {
        IngestLogsError::Clock(message) => Status::internal(message),
        IngestLogsError::Append(error) => match error.kind() {
            AppendErrorKind::InvalidArgument => Status::invalid_argument(error.to_string()),
            AppendErrorKind::ResourceExhausted => Status::resource_exhausted(error.to_string()),
            AppendErrorKind::Unavailable => Status::unavailable(error.to_string()),
            AppendErrorKind::Internal => Status::internal(error.to_string()),
        },
    }
}
