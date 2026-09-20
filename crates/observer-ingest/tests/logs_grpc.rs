use std::{
    net::SocketAddr,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use observer_ingest::{
    AcceptedBatch, AppendError, IngestSink, LogsIngestService, Signal, TokenDirectory,
};
use observer_protocol::otlp::{
    AnyValue, ExportLogsServiceRequest, KeyValue, LogsServiceClient, Resource, ResourceLogs,
    any_value,
};
use prost::Message;
use tokio::{
    net::TcpListener,
    sync::{Notify, oneshot},
    task::JoinHandle,
    time::timeout,
};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{Code, Request, transport::Server};

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

async fn spawn_server<S>(sink: Arc<S>) -> TestServer
where
    S: IngestSink,
{
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind test listener");
    let address = listener.local_addr().expect("read listener address");
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let service = LogsIngestService::new(sink, test_tokens()).into_server(MAX_MESSAGE_SIZE);
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

async fn connect(address: SocketAddr) -> LogsServiceClient<tonic::transport::Channel> {
    LogsServiceClient::connect(format!("http://{address}"))
        .await
        .expect("connect logs client")
}

fn export_request(
    authorization: Option<&str>,
    body: ExportLogsServiceRequest,
) -> Request<ExportLogsServiceRequest> {
    let mut request = Request::new(body);
    if let Some(authorization) = authorization {
        request.metadata_mut().insert(
            "authorization",
            authorization.parse().expect("authorization metadata"),
        );
    }
    request
}

fn logs_with_tenant_attribute(tenant: &str) -> ExportLogsServiceRequest {
    ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            resource: Some(Resource {
                attributes: vec![KeyValue {
                    key: "tenant.id".to_owned(),
                    value: Some(AnyValue {
                        value: Some(any_value::Value::StringValue(tenant.to_owned())),
                    }),
                    ..Default::default()
                }],
                ..Default::default()
            }),
            ..Default::default()
        }],
    }
}

#[tokio::test]
async fn accepts_an_otlp_logs_request_over_grpc() {
    let sink = Arc::new(RecordingSink::default());
    let server = spawn_server(Arc::clone(&sink)).await;
    let mut client = connect(server.address).await;
    let request = empty_logs_request();

    let response = client
        .export(export_request(Some(BEARER), request.clone()))
        .await
        .expect("export logs")
        .into_inner();

    assert!(response.partial_success.is_none());
    assert_eq!(sink.append_calls.load(Ordering::SeqCst), 1);
    {
        let batches = sink.batches.lock().expect("sink lock poisoned");
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].tenant_id, "tenant-a");
        assert_eq!(batches[0].signal, Signal::Logs);
        assert!(batches[0].received_at_unix_nanos > 0);
        assert_eq!(
            ExportLogsServiceRequest::decode(batches[0].payload.clone())
                .expect("decode stored payload"),
            request
        );
    }

    server.shutdown().await;
}

#[tokio::test]
async fn does_not_acknowledge_when_sink_fails() {
    let sink = Arc::new(FailingSink::default());
    let server = spawn_server(Arc::clone(&sink)).await;
    let mut client = connect(server.address).await;

    let error = client
        .export(export_request(Some(BEARER), empty_logs_request()))
        .await
        .expect_err("sink failure must fail export");

    assert_eq!(error.code(), Code::Unavailable);
    assert_eq!(error.message(), "durable append failed");
    assert_eq!(sink.append_calls.load(Ordering::SeqCst), 1);

    server.shutdown().await;
}

#[tokio::test]
async fn waits_for_sink_before_acknowledging() {
    let sink = Arc::new(BlockingSink::default());
    let append_started = sink.started.notified();
    let server = spawn_server(Arc::clone(&sink)).await;
    let mut client = connect(server.address).await;

    let export = tokio::spawn(async move {
        client
            .export(export_request(Some(BEARER), empty_logs_request()))
            .await
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
        .expect("join export task")
        .expect("export logs")
        .into_inner();

    assert!(response.partial_success.is_none());
    assert_eq!(sink.append_calls.load(Ordering::SeqCst), 1);

    server.shutdown().await;
}

#[tokio::test]
async fn missing_malformed_and_unknown_tokens_are_rejected_before_the_sink() {
    let sink = Arc::new(RecordingSink::default());
    let server = spawn_server(Arc::clone(&sink)).await;
    let mut client = connect(server.address).await;

    for authorization in [None, Some("Basic secret-a"), Some("Bearer unknown")] {
        let error = client
            .export(export_request(authorization, empty_logs_request()))
            .await
            .expect_err("unauthenticated");
        assert_eq!(error.code(), Code::Unauthenticated);
        assert_eq!(error.message(), "unauthenticated");
        assert!(!error.message().contains(SECRET));
        assert!(!error.message().contains("unknown"));
    }
    assert_eq!(sink.append_calls.load(Ordering::SeqCst), 0);

    server.shutdown().await;
}

#[tokio::test]
async fn telemetry_attributes_cannot_override_authenticated_tenant() {
    let sink = Arc::new(RecordingSink::default());
    let server = spawn_server(Arc::clone(&sink)).await;
    let mut client = connect(server.address).await;

    client
        .export(export_request(
            Some(BEARER),
            logs_with_tenant_attribute("attacker"),
        ))
        .await
        .expect("export logs");

    {
        let batches = sink.batches.lock().expect("sink lock poisoned");
        assert_eq!(batches[0].tenant_id, "tenant-a");
        assert_ne!(batches[0].tenant_id, "attacker");
    }

    server.shutdown().await;
}
