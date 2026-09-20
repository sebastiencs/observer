use std::sync::Arc;

use observer_protocol::{
    AppendErrorKind, IngestSink,
    otlp::{ExportLogsServiceRequest, ExportLogsServiceResponse, LogsService, LogsServiceServer},
};
use tonic::{Request, Response, Status};

use crate::{
    accept::{IngestLogsError, ingest_logs},
    auth::{AuthError, TokenDirectory},
};

/// OTLP/gRPC logs ingestion service.
#[derive(Debug)]
pub struct LogsIngestService<S> {
    sink: Arc<S>,
    tokens: TokenDirectory,
}

impl<S> LogsIngestService<S> {
    #[must_use]
    pub fn new(sink: Arc<S>, tokens: TokenDirectory) -> Self {
        Self { sink, tokens }
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
        let authorization = request
            .metadata()
            .get("authorization")
            .and_then(|value| value.to_str().ok());
        let tenant_id = self
            .tokens
            .authenticate(authorization)
            .map_err(status_from_auth)?;
        ingest_logs(&*self.sink, tenant_id, request.into_inner())
            .await
            .map(Response::new)
            .map_err(status_from_ingest)
    }
}

fn status_from_auth(error: AuthError) -> Status {
    Status::unauthenticated(error.to_string())
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
