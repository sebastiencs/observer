use std::{fmt, sync::Arc};

use axum::{
    Router,
    body::Bytes,
    extract::{DefaultBodyLimit, State},
    http::{
        HeaderMap, StatusCode,
        header::{AUTHORIZATION, CONTENT_TYPE, HeaderValue},
    },
    response::{IntoResponse, Response},
    routing::post,
};
use observer_protocol::{AppendErrorKind, IngestSink, otlp::ExportLogsServiceRequest};
use prost::Message;

use crate::{
    accept::{IngestLogsError, ingest_logs},
    auth::{AuthError, TokenDirectory},
};

const PROTOBUF_CONTENT_TYPE: &str = "application/x-protobuf";

/// OTLP/HTTP logs ingestion service (`POST /v1/logs`).
pub struct LogsHttpService<S> {
    sink: Arc<S>,
    tokens: TokenDirectory,
    max_body_bytes: usize,
}

impl<S> Clone for LogsHttpService<S> {
    fn clone(&self) -> Self {
        Self {
            sink: Arc::clone(&self.sink),
            tokens: self.tokens.clone(),
            max_body_bytes: self.max_body_bytes,
        }
    }
}

impl<S> fmt::Debug for LogsHttpService<S> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LogsHttpService")
            .field("tokens", &self.tokens)
            .field("max_body_bytes", &self.max_body_bytes)
            .finish_non_exhaustive()
    }
}

impl<S> LogsHttpService<S>
where
    S: IngestSink,
{
    #[must_use]
    pub fn new(sink: Arc<S>, tokens: TokenDirectory, max_body_bytes: usize) -> Self {
        Self {
            sink,
            tokens,
            max_body_bytes,
        }
    }

    pub fn into_router(self) -> Router {
        let max_body_bytes = self.max_body_bytes;
        Router::new()
            .route("/v1/logs", post(export_logs::<S>))
            .layer(DefaultBodyLimit::max(max_body_bytes))
            .with_state(self)
    }
}

async fn export_logs<S>(
    State(service): State<LogsHttpService<S>>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<ProtobufBody, HttpIngestError>
where
    S: IngestSink,
{
    let authorization = headers
        .get(AUTHORIZATION)
        .and_then(|value| value.to_str().ok());
    let tenant_id = service
        .tokens
        .authenticate(authorization)
        .map_err(HttpIngestError::from)?;

    if !is_protobuf_content_type(&headers) {
        return Err(HttpIngestError::UnsupportedMediaType);
    }

    let request =
        ExportLogsServiceRequest::decode(body).map_err(|_| HttpIngestError::InvalidProtobuf)?;

    let response = ingest_logs(&*service.sink, tenant_id, request)
        .await
        .map_err(HttpIngestError::from)?;

    Ok(ProtobufBody(response.encode_to_vec()))
}

fn is_protobuf_content_type(headers: &HeaderMap) -> bool {
    let Some(value) = headers.get(CONTENT_TYPE) else {
        return false;
    };
    let Ok(value) = value.to_str() else {
        return false;
    };
    let media_type = value.split(';').next().unwrap_or("").trim();
    media_type.eq_ignore_ascii_case(PROTOBUF_CONTENT_TYPE)
}

struct ProtobufBody(Vec<u8>);

impl IntoResponse for ProtobufBody {
    fn into_response(self) -> Response {
        (
            [(
                CONTENT_TYPE,
                HeaderValue::from_static(PROTOBUF_CONTENT_TYPE),
            )],
            self.0,
        )
            .into_response()
    }
}

enum HttpIngestError {
    Unauthenticated,
    UnsupportedMediaType,
    InvalidProtobuf,
    Clock(&'static str),
    Append(observer_protocol::AppendError),
}

impl From<AuthError> for HttpIngestError {
    fn from(error: AuthError) -> Self {
        match error {
            AuthError::Unauthenticated => Self::Unauthenticated,
        }
    }
}

impl From<IngestLogsError> for HttpIngestError {
    fn from(error: IngestLogsError) -> Self {
        match error {
            IngestLogsError::Clock(message) => Self::Clock(message),
            IngestLogsError::Append(error) => Self::Append(error),
        }
    }
}

impl IntoResponse for HttpIngestError {
    fn into_response(self) -> Response {
        let (status, message) = match self {
            Self::Unauthenticated => (
                StatusCode::UNAUTHORIZED,
                AuthError::Unauthenticated.to_string(),
            ),
            Self::UnsupportedMediaType => (
                StatusCode::UNSUPPORTED_MEDIA_TYPE,
                "unsupported content type".to_owned(),
            ),
            Self::InvalidProtobuf => (StatusCode::BAD_REQUEST, "invalid protobuf".to_owned()),
            Self::Clock(message) => (StatusCode::INTERNAL_SERVER_ERROR, message.to_owned()),
            Self::Append(error) => {
                let status = match error.kind() {
                    // Admission pressure is a client-visible retry signal, not WAL failure.
                    AppendErrorKind::ResourceExhausted => StatusCode::TOO_MANY_REQUESTS,
                    AppendErrorKind::Unavailable => StatusCode::SERVICE_UNAVAILABLE,
                    AppendErrorKind::InvalidArgument => StatusCode::BAD_REQUEST,
                    AppendErrorKind::Internal => StatusCode::INTERNAL_SERVER_ERROR,
                };
                (status, error.to_string())
            }
        };
        (status, message).into_response()
    }
}
