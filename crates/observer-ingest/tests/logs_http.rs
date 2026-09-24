use std::{
    net::SocketAddr,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use observer_ingest::{
    AcceptedBatch, AppendError, FUTURE_TIMESTAMP, IngestOptions, IngestSink, LogsHttpService,
    LogsIngestService, Signal, TokenDirectory,
};
use observer_protocol::otlp::{
    ExportLogsServiceRequest, ExportLogsServiceResponse, LogRecord, LogsServiceClient,
    ResourceLogs, ScopeLogs,
};
use prost::Message;
use reqwest::StatusCode;
use tokio::{
    net::TcpListener,
    sync::{Notify, oneshot},
    task::JoinHandle,
    time::timeout,
};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;

const MAX_MESSAGE_SIZE: usize = 16 * 1024 * 1024;
const TEST_TIMEOUT: Duration = Duration::from_secs(5);
const BEARER: &str = "Bearer secret-a";
const SECRET: &str = "secret-a";

fn test_tokens() -> TokenDirectory {
    TokenDirectory::new([(SECRET, "tenant-a")]).expect("tokens")
}

#[derive(Debug, Default)]
struct RecordingSink {
    batches: Mutex<Vec<AcceptedBatch>>,
    append_calls: AtomicUsize,
}

#[tonic::async_trait]
impl IngestSink for RecordingSink {
    async fn append(&self, batch: AcceptedBatch) -> Result<(), AppendError> {
        self.append_calls.fetch_add(1, Ordering::SeqCst);
        self.batches.lock().expect("sink lock poisoned").push(batch);
        Ok(())
    }
}

#[derive(Debug, Default)]
struct FailingSink {
    append_calls: AtomicUsize,
}

#[tonic::async_trait]
impl IngestSink for FailingSink {
    async fn append(&self, _batch: AcceptedBatch) -> Result<(), AppendError> {
        self.append_calls.fetch_add(1, Ordering::SeqCst);
        Err(AppendError::new("durable append failed"))
    }
}

#[derive(Debug, Default)]
struct BlockingSink {
    append_calls: AtomicUsize,
    started: Notify,
    release: Notify,
}

#[tonic::async_trait]
impl IngestSink for BlockingSink {
    async fn append(&self, _batch: AcceptedBatch) -> Result<(), AppendError> {
        self.append_calls.fetch_add(1, Ordering::SeqCst);
        self.started.notify_one();
        self.release.notified().await;
        Ok(())
    }
}

struct TestServer {
    address: SocketAddr,
    shutdown_tx: oneshot::Sender<()>,
    task: JoinHandle<()>,
}

impl TestServer {
    async fn shutdown(self) {
        self.shutdown_tx.send(()).expect("request server shutdown");
        self.task.await.expect("join server");
    }
}

async fn spawn_http<S>(sink: Arc<S>, max_body_bytes: usize) -> TestServer
where
    S: IngestSink,
{
    spawn_http_at(sink, max_body_bytes, IngestOptions::default()).await
}

async fn spawn_http_at<S>(sink: Arc<S>, max_body_bytes: usize, options: IngestOptions) -> TestServer
where
    S: IngestSink,
{
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind test listener");
    let address = listener.local_addr().expect("read listener address");
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let router = LogsHttpService::new(sink, test_tokens(), max_body_bytes, options).into_router();
    let task = tokio::spawn(async move {
        axum::serve(listener, router)
            .with_graceful_shutdown(async {
                let _ = shutdown_rx.await;
            })
            .await
            .expect("serve OTLP HTTP logs");
    });

    TestServer {
        address,
        shutdown_tx,
        task,
    }
}

async fn spawn_grpc<S>(sink: Arc<S>) -> TestServer
where
    S: IngestSink,
{
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind test listener");
    let address = listener.local_addr().expect("read listener address");
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let service = LogsIngestService::new(sink, test_tokens(), IngestOptions::default())
        .into_server(MAX_MESSAGE_SIZE);
    let task = tokio::spawn(async move {
        Server::builder()
            .add_service(service)
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                let _ = shutdown_rx.await;
            })
            .await
            .expect("serve OTLP logs");
    });

    TestServer {
        address,
        shutdown_tx,
        task,
    }
}

fn empty_logs_request() -> ExportLogsServiceRequest {
    ExportLogsServiceRequest {
        resource_logs: Vec::new(),
    }
}

async fn post_logs(address: SocketAddr, content_type: &str, body: Vec<u8>) -> reqwest::Response {
    post_logs_with_auth(address, Some(BEARER), content_type, body).await
}

async fn post_logs_with_auth(
    address: SocketAddr,
    authorization: Option<&str>,
    content_type: &str,
    body: Vec<u8>,
) -> reqwest::Response {
    let mut request = reqwest::Client::new()
        .post(format!("http://{address}/v1/logs"))
        .header(reqwest::header::CONTENT_TYPE, content_type)
        .body(body);
    if let Some(authorization) = authorization {
        request = request.header(reqwest::header::AUTHORIZATION, authorization);
    }
    request.send().await.expect("post logs")
}

#[tokio::test]
async fn accepts_an_otlp_logs_request_over_http() {
    let sink = Arc::new(RecordingSink::default());
    let server = spawn_http(Arc::clone(&sink), MAX_MESSAGE_SIZE).await;
    let request = empty_logs_request();

    let response = post_logs(
        server.address,
        "application/x-protobuf",
        request.encode_to_vec(),
    )
    .await;

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok()),
        Some("application/x-protobuf")
    );
    let decoded = ExportLogsServiceResponse::decode(response.bytes().await.expect("body"))
        .expect("decode response");
    assert!(decoded.partial_success.is_none());
    assert_eq!(sink.append_calls.load(Ordering::SeqCst), 1);
    {
        let batches = sink.batches.lock().expect("sink lock poisoned");
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].tenant_id, "tenant-a");
        assert_eq!(batches[0].signal, Signal::Logs);
        assert!(batches[0].received_at_unix_nanos > 0);
        assert_eq!(batches[0].payload.as_ref(), request.encode_to_vec());
        assert_eq!(
            ExportLogsServiceRequest::decode(batches[0].payload.clone())
                .expect("decode stored payload"),
            request
        );
    }

    server.shutdown().await;
}

#[tokio::test]
async fn http_and_grpc_store_equivalent_payload_bytes() {
    let sink = Arc::new(RecordingSink::default());
    let http = spawn_http(Arc::clone(&sink), MAX_MESSAGE_SIZE).await;
    let grpc = spawn_grpc(Arc::clone(&sink)).await;
    let request = empty_logs_request();

    let http_response = post_logs(
        http.address,
        "application/x-protobuf",
        request.encode_to_vec(),
    )
    .await;
    assert_eq!(http_response.status(), StatusCode::OK);

    let mut client = LogsServiceClient::connect(format!("http://{}", grpc.address))
        .await
        .expect("connect logs client");
    let mut export = tonic::Request::new(request.clone());
    export.metadata_mut().insert(
        "authorization",
        BEARER.parse().expect("authorization metadata"),
    );
    client.export(export).await.expect("export logs");

    {
        let batches = sink.batches.lock().expect("sink lock poisoned");
        assert_eq!(batches.len(), 2);
        assert_eq!(batches[0].payload, batches[1].payload);
        assert_eq!(batches[0].payload.as_ref(), request.encode_to_vec());
    }

    http.shutdown().await;
    grpc.shutdown().await;
}

#[tokio::test]
async fn rejects_invalid_protobuf() {
    let sink = Arc::new(RecordingSink::default());
    let server = spawn_http(Arc::clone(&sink), MAX_MESSAGE_SIZE).await;

    let response = post_logs(server.address, "application/x-protobuf", vec![0xff, 0x00]).await;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert_eq!(sink.append_calls.load(Ordering::SeqCst), 0);

    server.shutdown().await;
}

#[tokio::test]
async fn rejects_unsupported_content_type() {
    let sink = Arc::new(RecordingSink::default());
    let server = spawn_http(Arc::clone(&sink), MAX_MESSAGE_SIZE).await;

    let response = post_logs(
        server.address,
        "application/json",
        empty_logs_request().encode_to_vec(),
    )
    .await;

    assert_eq!(response.status(), StatusCode::UNSUPPORTED_MEDIA_TYPE);
    assert_eq!(sink.append_calls.load(Ordering::SeqCst), 0);

    server.shutdown().await;
}

#[tokio::test]
async fn rejects_oversized_body() {
    let sink = Arc::new(RecordingSink::default());
    let server = spawn_http(Arc::clone(&sink), 8).await;

    let response = post_logs(server.address, "application/x-protobuf", vec![0; 32]).await;

    assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    assert_eq!(sink.append_calls.load(Ordering::SeqCst), 0);

    server.shutdown().await;
}

#[tokio::test]
async fn does_not_acknowledge_when_sink_fails() {
    let sink = Arc::new(FailingSink::default());
    let server = spawn_http(Arc::clone(&sink), MAX_MESSAGE_SIZE).await;

    let response = post_logs(
        server.address,
        "application/x-protobuf",
        empty_logs_request().encode_to_vec(),
    )
    .await;

    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(
        response.text().await.expect("body"),
        "durable append failed"
    );
    assert_eq!(sink.append_calls.load(Ordering::SeqCst), 1);

    server.shutdown().await;
}

#[tokio::test]
async fn waits_for_sink_before_acknowledging() {
    let sink = Arc::new(BlockingSink::default());
    let append_started = sink.started.notified();
    let server = spawn_http(Arc::clone(&sink), MAX_MESSAGE_SIZE).await;

    let export = tokio::spawn({
        let address = server.address;
        let body = empty_logs_request().encode_to_vec();
        async move { post_logs(address, "application/x-protobuf", body).await }
    });

    timeout(TEST_TIMEOUT, append_started)
        .await
        .expect("append did not start");
    assert_eq!(sink.append_calls.load(Ordering::SeqCst), 1);
    assert!(
        !export.is_finished(),
        "export completed before append reached its acknowledgement boundary"
    );

    sink.release.notify_one();
    let response = timeout(TEST_TIMEOUT, export)
        .await
        .expect("export did not complete after append succeeded")
        .expect("join export task");

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(sink.append_calls.load(Ordering::SeqCst), 1);

    server.shutdown().await;
}

#[tokio::test]
async fn missing_malformed_and_unknown_tokens_are_rejected_before_the_sink() {
    let sink = Arc::new(RecordingSink::default());
    let server = spawn_http(Arc::clone(&sink), MAX_MESSAGE_SIZE).await;
    let body = empty_logs_request().encode_to_vec();

    for authorization in [None, Some("Basic secret-a"), Some("Bearer unknown")] {
        let response = post_logs_with_auth(
            server.address,
            authorization,
            "application/x-protobuf",
            body.clone(),
        )
        .await;
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        let text = response.text().await.expect("body");
        assert_eq!(text, "unauthenticated");
        assert!(!text.contains(SECRET));
        assert!(!text.contains("unknown"));
    }
    assert_eq!(sink.append_calls.load(Ordering::SeqCst), 0);

    server.shutdown().await;
}

fn timed_request(records: Vec<LogRecord>) -> ExportLogsServiceRequest {
    ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            scope_logs: vec![ScopeLogs {
                log_records: records,
                ..Default::default()
            }],
            ..Default::default()
        }],
    }
}

fn at(time: u64, observed: u64) -> LogRecord {
    LogRecord {
        time_unix_nano: time,
        observed_time_unix_nano: observed,
        ..Default::default()
    }
}

fn fixed(received: u64, skew: Duration) -> IngestOptions {
    IngestOptions {
        max_future_skew: skew,
        receive_time_unix_nano: Some(received),
    }
}

#[tokio::test]
async fn http_rejects_a_future_event_timestamp_before_append() {
    let sink = Arc::new(RecordingSink::default());
    let server = spawn_http_at(
        Arc::clone(&sink),
        MAX_MESSAGE_SIZE,
        fixed(1_000, Duration::from_nanos(100)),
    )
    .await;
    let response = post_logs(
        server.address,
        "application/x-protobuf",
        timed_request(vec![at(1_100, 0)]).encode_to_vec(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        sink.batches.lock().expect("lock")[0].received_at_unix_nanos,
        1_000
    );

    let response = post_logs(
        server.address,
        "application/x-protobuf",
        timed_request(vec![at(1, 0), at(1_101, 0)]).encode_to_vec(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert_eq!(response.text().await.expect("body"), FUTURE_TIMESTAMP);
    assert_eq!(sink.append_calls.load(Ordering::SeqCst), 1);

    let response = post_logs(
        server.address,
        "application/x-protobuf",
        timed_request(vec![at(0, 1_101)]).encode_to_vec(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert_eq!(sink.append_calls.load(Ordering::SeqCst), 1);

    let response = post_logs(
        server.address,
        "application/x-protobuf",
        timed_request(vec![at(1, 0), at(0, 0)]).encode_to_vec(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);

    let unauthenticated = post_logs_with_auth(
        server.address,
        None,
        "application/x-protobuf",
        timed_request(vec![at(u64::MAX, 0)]).encode_to_vec(),
    )
    .await;
    assert_eq!(unauthenticated.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(sink.append_calls.load(Ordering::SeqCst), 2);
    server.shutdown().await;

    let sink = Arc::new(RecordingSink::default());
    let server = spawn_http_at(
        Arc::clone(&sink),
        MAX_MESSAGE_SIZE,
        fixed(u64::MAX, Duration::from_nanos(1)),
    )
    .await;
    let response = post_logs(
        server.address,
        "application/x-protobuf",
        timed_request(vec![at(u64::MAX, 0)]).encode_to_vec(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    server.shutdown().await;

    let server = spawn_http_at(
        Arc::new(RecordingSink::default()),
        MAX_MESSAGE_SIZE,
        fixed(u64::MAX - 5, Duration::from_nanos(u64::MAX)),
    )
    .await;
    let response = post_logs(
        server.address,
        "application/x-protobuf",
        timed_request(vec![at(u64::MAX, 0)]).encode_to_vec(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    server.shutdown().await;
}
