use std::{fs, net::SocketAddr, sync::Arc, time::Duration};

use observer_ingest::{IngestSink, LogsIngestService, TokenDirectory};
use observer_protocol::otlp::{ExportLogsServiceRequest, LogsServiceClient};
use observer_wal::{
    AsyncWal, SEGMENT_HEADER_SIZE, WalIoHooks, WalWriterConfig, decode, encoded_frame_size,
};
use prost::Message;
use tokio::{net::TcpListener, sync::oneshot, task::JoinHandle, time::timeout};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{Code, Request, transport::Server};

const MAX_MESSAGE_SIZE: usize = 16 * 1024 * 1024;
const TEST_TIMEOUT: Duration = Duration::from_secs(5);
const BEARER: &str = "Bearer secret-a";

fn test_tokens() -> TokenDirectory {
    TokenDirectory::new([("secret-a", "tenant-a")]).expect("tokens")
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

fn authorized(body: ExportLogsServiceRequest) -> Request<ExportLogsServiceRequest> {
    let mut request = Request::new(body);
    request.metadata_mut().insert(
        "authorization",
        BEARER.parse().expect("authorization metadata"),
    );
    request
}

async fn wait_until(mut predicate: impl FnMut() -> bool) {
    timeout(TEST_TIMEOUT, async {
        while !predicate() {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("condition was not met");
}

#[tokio::test]
async fn accepts_an_otlp_logs_request_against_a_real_wal() {
    let dir = tempfile::tempdir().expect("tempdir");
    let wal = Arc::new(AsyncWal::open(WalWriterConfig::new(dir.path())).expect("open wal"));
    let server = spawn_server(Arc::clone(&wal)).await;
    let mut client = connect(server.address).await;
    let request = empty_logs_request();

    let response = client
        .export(authorized(request.clone()))
        .await
        .expect("export logs")
        .into_inner();

    assert!(response.partial_success.is_none());
    server.shutdown().await;
    wal.shutdown().await.expect("wal shutdown");

    let bytes = fs::read(dir.path().join("lane-0000/00000000000000000000.open")).expect("segment");
    let (frame, _) = decode(&bytes[SEGMENT_HEADER_SIZE..]).expect("decode");
    assert_eq!(frame.tenant_id, "tenant-a");
    assert_eq!(
        ExportLogsServiceRequest::decode(frame.payload).expect("decode stored payload"),
        request
    );
}

#[tokio::test]
async fn does_not_acknowledge_when_wal_sync_fails() {
    let dir = tempfile::tempdir().expect("tempdir");
    let hooks = Arc::new(WalIoHooks::new());
    hooks.fail_next_sync();
    let wal = Arc::new(
        AsyncWal::open_with_hooks(WalWriterConfig::new(dir.path()), Arc::clone(&hooks))
            .expect("open wal"),
    );
    let server = spawn_server(Arc::clone(&wal)).await;
    let mut client = connect(server.address).await;

    let error = client
        .export(authorized(empty_logs_request()))
        .await
        .expect_err("wal failure must fail export");

    assert_eq!(error.code(), Code::Unavailable);
    assert_eq!(error.message(), "WAL I/O error: injected sync error");

    server.shutdown().await;
    assert!(wal.shutdown().await.is_err());
}

#[tokio::test]
async fn waits_for_wal_sync_before_acknowledging() {
    let dir = tempfile::tempdir().expect("tempdir");
    let hooks = Arc::new(WalIoHooks::new());
    hooks.hold_next_sync();
    let wal = Arc::new(
        AsyncWal::open_with_hooks(WalWriterConfig::new(dir.path()), Arc::clone(&hooks))
            .expect("open wal"),
    );
    let server = spawn_server(Arc::clone(&wal)).await;
    let mut client = connect(server.address).await;

    let export = tokio::spawn(async move { client.export(authorized(empty_logs_request())).await });

    timeout(TEST_TIMEOUT, hooks.sync_started().notified())
        .await
        .expect("sync did not start");
    assert!(
        !export.is_finished(),
        "export completed before WAL sync reached its acknowledgement boundary"
    );

    hooks.release_sync();
    let response = timeout(TEST_TIMEOUT, export)
        .await
        .expect("export did not complete after sync succeeded")
        .expect("join export task")
        .expect("export logs")
        .into_inner();

    assert!(response.partial_success.is_none());
    server.shutdown().await;
    wal.shutdown().await.expect("wal shutdown");
}

#[tokio::test]
async fn saturating_wal_admission_returns_resource_exhausted() {
    let dir = tempfile::tempdir().expect("tempdir");
    let hooks = Arc::new(WalIoHooks::new());
    hooks.hold_admission();
    let mut config = WalWriterConfig::new(dir.path());
    let payload = empty_logs_request().encode_to_vec();
    config.max_queued_bytes =
        encoded_frame_size("tenant-a".len(), payload.len()).expect("admission cost");
    let wal = Arc::new(AsyncWal::open_with_hooks(config, Arc::clone(&hooks)).expect("open wal"));
    let server = spawn_server(Arc::clone(&wal)).await;

    timeout(TEST_TIMEOUT, hooks.admission_started().notified())
        .await
        .expect("writer did not pause");

    let first_address = server.address;
    let first = tokio::spawn(async move {
        let mut client = connect(first_address).await;
        client.export(authorized(empty_logs_request())).await
    });
    wait_until(|| wal.queued_bytes() > 0).await;

    let mut client = connect(server.address).await;
    let error = client
        .export(authorized(empty_logs_request()))
        .await
        .expect_err("saturated admission");
    assert_eq!(error.code(), Code::ResourceExhausted);

    hooks.release_admission();
    first.await.expect("join").expect("first export");
    server.shutdown().await;
    wal.shutdown().await.expect("wal shutdown");
}
