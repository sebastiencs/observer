#![cfg(unix)]

mod support;

use std::{fs, path::Path, time::Duration};

use arrow_array::{Array, StringArray};
use observer_protocol::otlp::{
    AnyValue, ExportLogsServiceRequest, LogRecord, LogsServiceClient, ResourceLogs, ScopeLogs,
    any_value,
};
use observer_storage::{Scan, Store, SystemClock};
use observer_wal::{WalCheckpoint, tenant_wal_directory};
use support::{
    BEARER, Observerd, SECOND_BEARER, SECOND_TENANT, TENANT, TEST_TIMEOUT, data_directory,
    post_logs, post_logs_as, reserve_ports, start_ready, wait_checkpoint, write_tuned,
};
use tokio::time::timeout;
use tonic::Request;

#[tokio::test]
async fn http_and_grpc_rows_stay_isolated_across_restart() {
    timeout(TEST_TIMEOUT * 3, lifecycle())
        .await
        .expect("lifecycle timed out");
}

async fn lifecycle() {
    let root = tempfile::tempdir().expect("tempdir");
    let wal_directory = root.path().join("wal");
    let config_path = root.path().join("observerd.toml");
    let listen = reserve_ports();
    write_tuned(&config_path, &wal_directory, listen, 0, 1);

    let mut daemon = start_ready(&config_path, listen).await;
    post_logs(listen.http, &marked("http-before")).await;
    export_grpc(listen.grpc, BEARER, &marked("grpc-before")).await;
    post_logs_as(listen.http, SECOND_BEARER, &marked("other-before")).await;
    wait_checkpoint(&wal_directory, TENANT, 2).await;
    wait_checkpoint(&wal_directory, SECOND_TENANT, 1).await;
    assert_eq!(
        bodies(&data_directory(&wal_directory), TENANT),
        vec![
            Some("http-before".to_owned()),
            Some("grpc-before".to_owned())
        ]
    );
    assert_eq!(
        bodies(&data_directory(&wal_directory), SECOND_TENANT),
        vec![Some("other-before".to_owned())]
    );
    assert!(published_file_exists(
        &data_directory(&wal_directory),
        TENANT,
        ".parquet"
    ));
    assert!(published_file_exists(
        &data_directory(&wal_directory),
        TENANT,
        ".commit"
    ));
    daemon.terminate().await;

    let mut daemon = start_ready(&config_path, listen).await;
    assert_eq!(
        bodies(&data_directory(&wal_directory), TENANT),
        vec![
            Some("http-before".to_owned()),
            Some("grpc-before".to_owned())
        ]
    );
    post_logs(listen.http, &marked("http-after")).await;
    wait_checkpoint(&wal_directory, TENANT, 3).await;
    daemon.terminate().await;

    let mut daemon = start_ready(&config_path, listen).await;
    assert_eq!(
        bodies(&data_directory(&wal_directory), TENANT),
        vec![
            Some("http-before".to_owned()),
            Some("grpc-before".to_owned()),
            Some("http-after".to_owned())
        ]
    );
    assert_eq!(
        bodies(&data_directory(&wal_directory), SECOND_TENANT),
        vec![Some("other-before".to_owned())]
    );
    daemon.terminate().await;
    assert_eq!(support::frames_on_disk(&wal_directory, TENANT).len(), 3);
    assert_eq!(
        support::frames_on_disk(&wal_directory, SECOND_TENANT).len(),
        1
    );
}

#[tokio::test]
async fn active_tail_appears_once_after_sigterm() {
    timeout(TEST_TIMEOUT * 2, active_tail())
        .await
        .expect("active tail timed out");
}

async fn active_tail() {
    let root = tempfile::tempdir().expect("tempdir");
    let wal_directory = root.path().join("wal");
    let config_path = root.path().join("observerd.toml");
    let listen = reserve_ports();
    write_tuned(&config_path, &wal_directory, listen, 100_000, 1);
    let mut daemon = start_ready(&config_path, listen).await;
    post_logs(listen.http, &marked("still-active")).await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(checkpoint(&wal_directory, TENANT), 0);
    assert!(!published_file_exists(
        &data_directory(&wal_directory),
        TENANT,
        ".commit"
    ));
    daemon.terminate().await;
    assert_eq!(checkpoint(&wal_directory, TENANT), 1);
    assert_eq!(
        bodies(&data_directory(&wal_directory), TENANT),
        vec![Some("still-active".to_owned())]
    );
}

#[tokio::test]
async fn startup_fails_when_either_filesystem_is_full() {
    let root = tempfile::tempdir().expect("tempdir");
    let wal_directory = root.path().join("wal");
    let config_path = root.path().join("observerd.toml");
    let listen = reserve_ports();
    write_tuned(&config_path, &wal_directory, listen, 100_000, u64::MAX);
    let mut daemon = Observerd::spawn(&config_path);
    let status = daemon.wait_exit().await;
    assert!(!status.success(), "{status}");
}

fn marked(body: &str) -> ExportLogsServiceRequest {
    ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            scope_logs: vec![ScopeLogs {
                log_records: vec![LogRecord {
                    time_unix_nano: 1,
                    body: Some(AnyValue {
                        value: Some(any_value::Value::StringValue(body.to_owned())),
                    }),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
    }
}

async fn export_grpc(
    address: std::net::SocketAddr,
    bearer: &str,
    request: &ExportLogsServiceRequest,
) {
    let mut client = LogsServiceClient::connect(format!("http://{address}"))
        .await
        .expect("connect grpc");
    let mut tonic_request = Request::new(request.clone());
    tonic_request.metadata_mut().insert(
        "authorization",
        bearer.parse().expect("authorization metadata"),
    );
    let response = client
        .export(tonic_request)
        .await
        .expect("export")
        .into_inner();
    assert!(response.partial_success.is_none());
}

fn checkpoint(wal_directory: &Path, tenant: &str) -> u64 {
    checkpoint_directory(&tenant_wal_directory(wal_directory, tenant)).unwrap_or(0)
}

fn checkpoint_directory(directory: &Path) -> Option<u64> {
    WalCheckpoint::load(directory)
        .ok()
        .map(|checkpoint| checkpoint.cursor().next_sequence())
}

fn bodies(data: &Path, tenant: &str) -> Vec<Option<String>> {
    let store = Store::open(
        data,
        tenant,
        observer_storage::MemtableConfig {
            max_rows: 100,
            max_bytes: u64::MAX,
            max_age: Duration::from_secs(60),
            max_frozen: 2,
            max_dynamic_columns: 32,
        },
        std::sync::Arc::new(SystemClock),
    )
    .expect("store");
    let snapshot = store.pin().expect("snapshot");
    let batches = store.scan(&snapshot, &Scan::default()).expect("scan");
    batches
        .iter()
        .flat_map(|batch| {
            let column = batch
                .column_by_name(observer_storage::COLUMN_BODY)
                .expect("body")
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("utf8");
            (0..column.len())
                .map(|row| (!column.is_null(row)).then(|| column.value(row).to_owned()))
        })
        .collect()
}

fn published_file_exists(data: &Path, tenant: &str, suffix: &str) -> bool {
    let tenant_dir = data.join("tenants").join(tenant);
    files_with_suffix(&tenant_dir, suffix)
}

fn files_with_suffix(directory: &Path, suffix: &str) -> bool {
    if !directory.exists() {
        return false;
    }
    for entry in fs::read_dir(directory).expect("read dir") {
        let path = entry.expect("entry").path();
        if path.is_dir() && files_with_suffix(&path, suffix) {
            return true;
        }
        if path
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| name.ends_with(suffix))
        {
            return true;
        }
    }
    false
}
