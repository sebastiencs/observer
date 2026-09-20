use std::sync::{Arc, Mutex};

use observer_ingest::{AcceptedBatch, AppendError, IngestSink, LogsIngestService, Signal};
use observer_protocol::otlp::{ExportLogsServiceRequest, LogsServiceClient};
use prost::Message;
use tokio::{net::TcpListener, sync::oneshot};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;

#[derive(Debug, Default)]
struct RecordingSink {
    batches: Mutex<Vec<AcceptedBatch>>,
}

#[tonic::async_trait]
impl IngestSink for RecordingSink {
    async fn append(&self, batch: AcceptedBatch) -> Result<(), AppendError> {
        self.batches.lock().expect("sink lock poisoned").push(batch);
        Ok(())
    }
}

#[tokio::test]
async fn accepts_an_otlp_logs_request_over_grpc() {
    let sink = Arc::new(RecordingSink::default());
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind test listener");
    let address = listener.local_addr().expect("read listener address");
    let (shutdown_tx, shutdown_rx) = oneshot::channel();

    let service =
        LogsIngestService::new(Arc::clone(&sink), "tenant-a").into_server(16 * 1024 * 1024);
    let server = tokio::spawn(async move {
        Server::builder()
            .add_service(service)
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                let _ = shutdown_rx.await;
            })
            .await
            .expect("serve OTLP logs");
    });

    let mut client = LogsServiceClient::connect(format!("http://{address}"))
        .await
        .expect("connect logs client");
    let request = ExportLogsServiceRequest {
        resource_logs: Vec::new(),
    };

    let response = client
        .export(request.clone())
        .await
        .expect("export logs")
        .into_inner();

    assert!(response.partial_success.is_none());
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

    shutdown_tx.send(()).expect("request server shutdown");
    server.await.expect("join server");
}
