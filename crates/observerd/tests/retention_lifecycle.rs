#![cfg(unix)]

mod support;

use std::{
    fs,
    path::Path,
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use bytes::Bytes;
use observer_ingest::FUTURE_TIMESTAMP;
use observer_protocol::otlp::{
    AnyValue, ExportLogsServiceRequest, LogRecord, ResourceLogs, ScopeLogs, any_value,
};
use observer_protocol::{AcceptedBatch, Signal};
use observer_storage::{
    DynamicLimits, ManualClock, MemtableConfig, PublishOptions, RetireOptions, Store,
    decode_logs_frame,
};
use observer_wal::{Frame, FrameSignal, Wal, WalConfig};
use prost::Message;
use reqwest::StatusCode;
use support::{
    BEARER, PolicyLimits, SECOND_BEARER, SECOND_TENANT, StorageLimits, TENANT, TEST_TIMEOUT,
    column_strings, data_directory, post_logs, post_logs_as, post_logs_raw, post_query,
    reserve_ports, sql_body, start_ready, write_policy,
};
use tokio::time::timeout;

const PROJECTION: &str = "SELECT body FROM logs ORDER BY body";

#[tokio::test]
async fn recent_receive_time_is_kept_and_a_future_event_is_rejected() {
    timeout(TEST_TIMEOUT * 3, kept_and_future())
        .await
        .expect("retention keep timed out");
}

#[tokio::test]
async fn short_retention_retires_one_tenant_and_keeps_the_other() {
    timeout(TEST_TIMEOUT * 3, tenant_isolation())
        .await
        .expect("tenant retention timed out");
}

#[tokio::test]
async fn same_hour_files_retire_only_the_older_receive_time() {
    timeout(TEST_TIMEOUT * 3, same_hour())
        .await
        .expect("same-hour retention timed out");
}

#[tokio::test]
async fn startup_collects_a_file_retired_before_unlink() {
    timeout(TEST_TIMEOUT * 3, crash_before_unlink())
        .await
        .expect("startup collection timed out");
}

async fn kept_and_future() {
    let root = tempfile::tempdir().expect("tempdir");
    let wal_directory = root.path().join("wal");
    let config_path = root.path().join("observerd.toml");
    let listen = reserve_ports();
    write_policy(
        &config_path,
        &wal_directory,
        listen,
        &StorageLimits::default(),
        &PolicyLimits {
            default_ms: 3_600_000,
            janitor_interval_ms: 200,
            max_future_skew_ms: 1_000,
            tenants: Vec::new(),
        },
    );
    let mut daemon = start_ready(&config_path, listen).await;
    post_logs(listen.http, &log("backfill", 1_000_000_000_000_000_000)).await;
    wait_for_bodies(listen.query, BEARER, &["backfill"]).await;
    let rejected = post_logs_raw(
        listen.http,
        Some(BEARER),
        Some("application/x-protobuf"),
        log("future", u64::MAX).encode_to_vec(),
    )
    .await;
    assert_eq!(rejected.status, StatusCode::BAD_REQUEST);
    assert_eq!(rejected.text(), FUTURE_TIMESTAMP);

    let start = Instant::now();
    while start.elapsed() < Duration::from_millis(600) {
        assert_eq!(
            bodies(listen.query, BEARER).await,
            vec!["backfill".to_owned()]
        );
        let ready = reqwest::Client::new()
            .get(format!("http://{}/ready", listen.admin))
            .send()
            .await
            .expect("ready");
        assert_eq!(ready.status(), StatusCode::OK);
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    daemon.terminate().await;

    let mut daemon = start_ready(&config_path, listen).await;
    assert_eq!(
        bodies(listen.query, BEARER).await,
        vec!["backfill".to_owned()]
    );
    daemon.terminate().await;
}

async fn tenant_isolation() {
    let root = tempfile::tempdir().expect("tempdir");
    let wal_directory = root.path().join("wal");
    let data = data_directory(&wal_directory);
    let config_path = root.path().join("observerd.toml");
    let listen = reserve_ports();
    write_policy(
        &config_path,
        &wal_directory,
        listen,
        &StorageLimits {
            max_rows: 0,
            ..StorageLimits::default()
        },
        &PolicyLimits {
            default_ms: 1,
            janitor_interval_ms: 1_500,
            max_future_skew_ms: 86_400_000,
            tenants: vec![(SECOND_TENANT.to_owned(), 0)],
        },
    );
    let mut daemon = start_ready(&config_path, listen).await;
    post_logs(listen.http, &log("expire", 1_700_000_000_000_000_000)).await;
    post_logs_as(
        listen.http,
        SECOND_BEARER,
        &log("forever", 1_700_000_000_000_000_000),
    )
    .await;
    wait_for_bodies(listen.query, BEARER, &["expire"]).await;
    let in_flight = tokio::spawn(bodies(listen.query, BEARER));
    wait_for_bodies(listen.query, BEARER, &[]).await;
    let overlapped = in_flight.await.expect("query");
    assert!(
        overlapped.is_empty() || overlapped == ["expire".to_owned()],
        "{overlapped:?}"
    );
    assert_eq!(
        bodies(listen.query, SECOND_BEARER).await,
        vec!["forever".to_owned()]
    );
    wait_until(|| count_suffix(&data, TENANT, ".parquet") == 0);
    assert!(count_suffix(&data, TENANT, ".commit") >= 1);
    assert!(count_suffix(&data, TENANT, ".retire") >= 1);
    assert!(count_suffix(&data, SECOND_TENANT, ".parquet") >= 1);
    daemon.terminate().await;

    let mut daemon = start_ready(&config_path, listen).await;
    assert_eq!(bodies(listen.query, BEARER).await, Vec::<String>::new());
    assert_eq!(
        bodies(listen.query, SECOND_BEARER).await,
        vec!["forever".to_owned()]
    );
    daemon.terminate().await;
}

async fn same_hour() {
    let root = tempfile::tempdir().expect("tempdir");
    let wal_directory = root.path().join("wal");
    let data = data_directory(&wal_directory);
    let config_path = root.path().join("observerd.toml");
    let hour = 1_700_000_000_000_000_000;
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    let now = u64::try_from(now).expect("nanos");
    plant(
        &wal_directory,
        &data,
        TENANT,
        &[(hour, "old-receive", 1_000), (hour, "new-receive", now)],
    );
    let listen = reserve_ports();
    write_policy(
        &config_path,
        &wal_directory,
        listen,
        &StorageLimits::default(),
        &PolicyLimits {
            default_ms: 60_000,
            janitor_interval_ms: 3_600_000,
            max_future_skew_ms: 86_400_000,
            tenants: Vec::new(),
        },
    );
    let mut daemon = start_ready(&config_path, listen).await;
    assert_eq!(
        bodies(listen.query, BEARER).await,
        vec!["new-receive".to_owned()]
    );
    assert_eq!(count_suffix(&data, TENANT, ".parquet"), 1);
    assert_eq!(count_suffix(&data, TENANT, ".commit"), 2);
    assert_eq!(count_suffix(&data, TENANT, ".retire"), 1);
    daemon.terminate().await;

    let mut daemon = start_ready(&config_path, listen).await;
    assert_eq!(
        bodies(listen.query, BEARER).await,
        vec!["new-receive".to_owned()]
    );
    assert_eq!(count_suffix(&data, TENANT, ".parquet"), 1);
    daemon.terminate().await;
}

async fn crash_before_unlink() {
    let root = tempfile::tempdir().expect("tempdir");
    let wal_directory = root.path().join("wal");
    let data = data_directory(&wal_directory);
    let config_path = root.path().join("observerd.toml");
    plant(&wal_directory, &data, TENANT, &[(1, "orphaned", 1_000)]);
    let store =
        Store::open(&data, TENANT, memtable(), Arc::new(ManualClock::new(0))).expect("open");
    store
        .retire_before(u64::MAX, &RetireOptions::default())
        .expect("retire");
    drop(store);
    assert_eq!(count_suffix(&data, TENANT, ".parquet"), 1);
    assert_eq!(count_suffix(&data, TENANT, ".retire"), 1);

    let listen = reserve_ports();
    write_policy(
        &config_path,
        &wal_directory,
        listen,
        &StorageLimits::default(),
        &PolicyLimits {
            default_ms: 0,
            janitor_interval_ms: 3_600_000,
            max_future_skew_ms: 86_400_000,
            tenants: Vec::new(),
        },
    );
    let mut daemon = start_ready(&config_path, listen).await;
    assert_eq!(bodies(listen.query, BEARER).await, Vec::<String>::new());
    assert_eq!(count_suffix(&data, TENANT, ".parquet"), 0);
    assert_eq!(count_suffix(&data, TENANT, ".commit"), 1);
    assert_eq!(count_suffix(&data, TENANT, ".retire"), 1);
    daemon.terminate().await;

    let mut daemon = start_ready(&config_path, listen).await;
    assert_eq!(count_suffix(&data, TENANT, ".parquet"), 0);
    assert_eq!(count_suffix(&data, TENANT, ".retire"), 1);
    daemon.terminate().await;
}

fn plant(wal_directory: &Path, data: &Path, tenant: &str, rows: &[(u64, &str, u64)]) {
    let store = Store::open(data, tenant, memtable(), Arc::new(ManualClock::new(0))).expect("open");
    let mut wal = Wal::open(WalConfig {
        directory: observer_wal::tenant_wal_directory(wal_directory, tenant),
        max_entry_bytes: 1024 * 1024,
        target_segment_bytes: 256 * 1024 * 1024,
    })
    .expect("wal");
    for (sequence, (time, body, received)) in rows.iter().enumerate() {
        let sequence = u64::try_from(sequence).expect("sequence");
        let payload = Bytes::from(log(body, *time).encode_to_vec());
        let receipt = wal
            .append(AcceptedBatch {
                tenant_id: tenant.to_owned(),
                signal: Signal::Logs,
                received_at_unix_nanos: *received,
                payload: payload.clone(),
            })
            .expect("wal append");
        assert_eq!(receipt.sequence, sequence);
        let logs = decode_logs_frame(
            &Frame {
                sequence,
                signal: FrameSignal::Logs,
                received_at_unix_nanos: *received,
                tenant_id: tenant.to_owned(),
                payload,
            },
            DynamicLimits {
                max_depth: 4,
                max_columns: 32,
            },
        )
        .expect("decode");
        store.append(&logs).expect("append");
        store.rotate().expect("rotate");
        store
            .publish(&PublishOptions::default())
            .expect("publish")
            .expect("commit");
    }
}

fn memtable() -> MemtableConfig {
    MemtableConfig {
        max_rows: 100,
        max_bytes: u64::MAX,
        max_age: Duration::from_secs(60),
        max_frozen: 4,
        max_dynamic_columns: 32,
    }
}

fn log(body: &str, time_unix_nano: u64) -> ExportLogsServiceRequest {
    ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            scope_logs: vec![ScopeLogs {
                log_records: vec![LogRecord {
                    time_unix_nano,
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

async fn bodies(address: std::net::SocketAddr, bearer: &str) -> Vec<String> {
    let response = post_query(address, Some(bearer), &sql_body(PROJECTION)).await;
    assert_eq!(response.status, StatusCode::OK, "{}", response.body);
    column_strings(&response.json(), "body")
}

async fn wait_for_bodies(address: std::net::SocketAddr, bearer: &str, expected: &[&str]) {
    let expected = expected
        .iter()
        .map(|body| (*body).to_owned())
        .collect::<Vec<_>>();
    let deadline = Instant::now() + TEST_TIMEOUT;
    let mut last = Vec::new();
    while Instant::now() < deadline {
        last = bodies(address, bearer).await;
        if last == expected {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("bodies were {last:?}, expected {expected:?}");
}

fn wait_until(mut ready: impl FnMut() -> bool) {
    let deadline = Instant::now() + TEST_TIMEOUT;
    while Instant::now() < deadline {
        if ready() {
            return;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    panic!("condition was not met");
}

fn count_suffix(data: &Path, tenant: &str, suffix: &str) -> usize {
    let directory = data.join("tenants").join(tenant);
    let mut count = 0;
    count_suffix_at(&directory, suffix, &mut count);
    count
}

fn count_suffix_at(directory: &Path, suffix: &str, count: &mut usize) {
    if !directory.exists() {
        return;
    }
    for entry in fs::read_dir(directory).expect("read dir") {
        let path = entry.expect("entry").path();
        if path.is_dir() {
            count_suffix_at(&path, suffix, count);
        } else if path
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| name.ends_with(suffix))
        {
            *count += 1;
        }
    }
}
