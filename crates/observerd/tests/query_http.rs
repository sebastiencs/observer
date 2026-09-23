#![cfg(unix)]

mod support;

use std::{
    path::Path,
    time::{Duration, Instant},
};

use observer_protocol::otlp::{
    AnyValue, ExportLogsServiceRequest, LogRecord, ResourceLogs, ScopeLogs, any_value,
};
use observer_wal::{WalCheckpoint, tenant_wal_directory};
use serde_json::{Value, json};
use support::{
    BEARER, ListenAddrs, Observerd, SECOND_BEARER, TEST_TIMEOUT, post_logs, post_logs_as,
    post_query, reserve_ports, write_config, write_response_cap,
};
use tokio::time::timeout;

const START_ATTEMPTS: usize = 3;
const EVENT_TIME: &str = "1700000000000000000";
const LATER_TIME: &str = "1700000000000000002";
const OTHER_TIME: &str = "1700000000000000001";
const PROJECTION: &str = "SELECT body, event_time_unix_nano FROM logs ORDER BY body";

#[tokio::test]
async fn http_queries_are_tenant_scoped_and_survive_restart() {
    timeout(TEST_TIMEOUT * 4, tenant_scope_and_restart())
        .await
        .expect("query restart scenario timed out");
}

#[tokio::test]
async fn http_queries_honor_client_limits_cors_and_the_response_cap() {
    timeout(TEST_TIMEOUT * 3, limits_and_cors())
        .await
        .expect("query limits scenario timed out");
}

#[tokio::test]
async fn http_queries_run_while_logs_are_ingested() {
    timeout(TEST_TIMEOUT * 2, ingest_and_query())
        .await
        .expect("concurrent query scenario timed out");
}

async fn tenant_scope_and_restart() {
    let root = tempfile::tempdir().expect("tempdir");
    let wal_directory = root.path().join("wal");
    let config_path = root.path().join("observerd.toml");
    let (listen, mut daemon) = start_with_retries(&config_path, &wal_directory).await;

    post_logs(listen.http, &marked("alpha", 1_700_000_000_000_000_000)).await;
    post_logs_as(
        listen.http,
        SECOND_BEARER,
        &marked("beta", 1_700_000_000_000_000_001),
    )
    .await;
    wait_for_bodies(listen.query, BEARER, &["alpha"]).await;
    wait_for_bodies(listen.query, SECOND_BEARER, &["beta"]).await;

    let own = post_query(listen.query, Some(BEARER), &sql_body(PROJECTION)).await;
    assert_eq!(own.status, reqwest::StatusCode::OK, "{}", own.body);
    assert_eq!(
        own.headers
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok()),
        Some("application/json")
    );
    assert_eq!(
        own.headers
            .get(reqwest::header::ACCESS_CONTROL_ALLOW_ORIGIN)
            .and_then(|value| value.to_str().ok()),
        Some("*")
    );
    let document = own.json();
    assert_eq!(
        document["schema"],
        json!([
            {"name": "body", "type": "Utf8", "nullable": true},
            {"name": "event_time_unix_nano", "type": "UInt64", "nullable": false}
        ])
    );
    assert_eq!(
        document["rows"],
        json!([{"body": "alpha", "event_time_unix_nano": EVENT_TIME}])
    );
    assert_metrics(&document["metrics"], 1);
    assert!(!own.body.contains("beta"));

    let other = post_query(listen.query, Some(SECOND_BEARER), &sql_body(PROJECTION)).await;
    assert_eq!(other.status, reqwest::StatusCode::OK, "{}", other.body);
    assert_eq!(
        other.json()["rows"],
        json!([{"body": "beta", "event_time_unix_nano": OTHER_TIME}])
    );
    assert!(!other.body.contains("alpha"));

    for bearer in [None, Some("Bearer nope")] {
        let rejected = post_query(listen.query, bearer, &sql_body("SELECT body FROM logs")).await;
        assert_eq!(rejected.status, reqwest::StatusCode::UNAUTHORIZED);
        assert_eq!(rejected.body, r#"{"error":{"code":"unauthenticated"}}"#);
        assert_eq!(
            rejected
                .headers
                .get(reqwest::header::WWW_AUTHENTICATE)
                .and_then(|value| value.to_str().ok()),
            Some("Bearer")
        );
        assert!(!rejected.body.contains("nope"));
        assert!(!rejected.body.contains("secret"));
    }

    daemon.terminate().await;
    let mut daemon = restart(&config_path, listen).await;
    let restored = post_query(listen.query, Some(BEARER), &sql_body(PROJECTION)).await;
    assert_eq!(
        restored.status,
        reqwest::StatusCode::OK,
        "{}",
        restored.body
    );
    let restored_document = restored.json();
    assert_eq!(
        restored_document["rows"],
        json!([{"body": "alpha", "event_time_unix_nano": EVENT_TIME}])
    );
    let scanned = restored_document["metrics"]["files_scanned"]
        .as_str()
        .expect("files_scanned")
        .parse::<u64>()
        .expect("files scanned count");
    assert!(scanned >= 1, "restarted query scanned {scanned} files");

    post_logs(
        listen.http,
        &marked("alpha-again", 1_700_000_000_000_000_002),
    )
    .await;
    wait_for_bodies(listen.query, BEARER, &["alpha", "alpha-again"]).await;
    let both = post_query(listen.query, Some(BEARER), &sql_body(PROJECTION)).await;
    let both_document = both.json();
    assert_eq!(
        both_document["rows"],
        json!([
            {"body": "alpha", "event_time_unix_nano": EVENT_TIME},
            {"body": "alpha-again", "event_time_unix_nano": LATER_TIME}
        ])
    );
    assert_metrics(&both_document["metrics"], 2);
    let still_other = post_query(listen.query, Some(SECOND_BEARER), &sql_body(PROJECTION)).await;
    assert_eq!(
        still_other.json()["rows"],
        json!([{"body": "beta", "event_time_unix_nano": OTHER_TIME}])
    );
    daemon.terminate().await;
}

async fn limits_and_cors() {
    let root = tempfile::tempdir().expect("tempdir");
    let wal_directory = root.path().join("wal");
    let config_path = root.path().join("observerd.toml");
    let (listen, mut daemon) = start_with_retries(&config_path, &wal_directory).await;

    let preflight = reqwest::Client::new()
        .request(
            reqwest::Method::OPTIONS,
            format!("http://{}/v1/query", listen.query),
        )
        .header(reqwest::header::ORIGIN, "https://app.example")
        .header(reqwest::header::ACCESS_CONTROL_REQUEST_METHOD, "POST")
        .header(
            reqwest::header::ACCESS_CONTROL_REQUEST_HEADERS,
            "authorization,content-type",
        )
        .send()
        .await
        .expect("preflight");
    assert!(preflight.status().is_success(), "{}", preflight.status());
    let headers = preflight.headers();
    assert_eq!(
        headers
            .get(reqwest::header::ACCESS_CONTROL_ALLOW_ORIGIN)
            .and_then(|value| value.to_str().ok()),
        Some("*")
    );
    let methods = headers
        .get(reqwest::header::ACCESS_CONTROL_ALLOW_METHODS)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("")
        .to_ascii_lowercase();
    assert!(methods.contains("post"), "{methods}");
    assert!(methods.contains("options"), "{methods}");
    let allowed = headers
        .get(reqwest::header::ACCESS_CONTROL_ALLOW_HEADERS)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("")
        .to_ascii_lowercase();
    assert!(allowed.contains("authorization"), "{allowed}");
    assert!(allowed.contains("content-type"), "{allowed}");
    assert!(
        headers
            .get(reqwest::header::ACCESS_CONTROL_ALLOW_CREDENTIALS)
            .is_none()
    );

    let timed_out = post_query(
        listen.query,
        Some(BEARER),
        r#"{"sql":"SELECT body FROM logs","timeout_ms":0}"#,
    )
    .await;
    assert_eq!(timed_out.status, reqwest::StatusCode::REQUEST_TIMEOUT);
    assert_eq!(timed_out.body, r#"{"error":{"code":"timeout"}}"#);

    post_logs(listen.http, &marked("one", 1)).await;
    post_logs(listen.http, &marked("two", 2)).await;
    wait_for_bodies(listen.query, BEARER, &["one", "two"]).await;
    let limited = post_query(
        listen.query,
        Some(BEARER),
        r#"{"sql":"SELECT body FROM logs ORDER BY body","max_rows":1}"#,
    )
    .await;
    assert_eq!(limited.status, reqwest::StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(limited.body, r#"{"error":{"code":"row_limit"}}"#);
    assert!(!limited.body.contains("\"rows\""));
    daemon.terminate().await;

    let root = tempfile::tempdir().expect("tempdir");
    let wal_directory = root.path().join("wal");
    let config_path = root.path().join("observerd.toml");
    let listen = reserve_ports();
    write_response_cap(&config_path, &wal_directory, listen, 64);
    let mut daemon = start(&config_path, listen).await;
    post_logs(listen.http, &marked("too-big", 1)).await;
    wait_checkpoint(&wal_directory, support::TENANT, 1).await;
    let oversized = post_query(
        listen.query,
        Some(BEARER),
        &sql_body("SELECT body FROM logs"),
    )
    .await;
    assert_eq!(oversized.status, reqwest::StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(oversized.body, r#"{"error":{"code":"response_too_large"}}"#);
    assert!(!oversized.body.contains("too-big"));
    assert!(!oversized.body.contains("\"rows\""));
    daemon.terminate().await;
}

async fn ingest_and_query() {
    let root = tempfile::tempdir().expect("tempdir");
    let wal_directory = root.path().join("wal");
    let config_path = root.path().join("observerd.toml");
    let (listen, mut daemon) = start_with_retries(&config_path, &wal_directory).await;
    let expected = [
        "row-0", "row-1", "row-2", "row-3", "row-4", "row-5", "row-6", "row-7",
    ];
    let query_address = listen.query;
    let http_address = listen.http;
    let rows = expected;
    let reader = tokio::spawn(async move {
        let deadline = Instant::now() + TEST_TIMEOUT;
        let mut last = String::new();
        while Instant::now() < deadline {
            let response = post_query(query_address, Some(BEARER), &sql_body(PROJECTION)).await;
            assert_eq!(
                response.status,
                reqwest::StatusCode::OK,
                "{}",
                response.body
            );
            last = response.body;
            let document = serde_json::from_str::<Value>(&last).expect("query json");
            let bodies = bodies_of(&document);
            assert!(
                bodies.windows(2).all(|pair| pair[0] <= pair[1]),
                "rows were not ordered: {bodies:?}"
            );
            assert!(
                bodies.iter().all(|body| rows.contains(&body.as_str())),
                "unexpected row in {bodies:?}"
            );
            if bodies.iter().map(String::as_str).eq(rows.iter().copied()) {
                assert_metrics(
                    &document["metrics"],
                    u64::try_from(rows.len()).expect("len"),
                );
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("concurrent query never saw {rows:?}: {last}");
    });
    for (index, body) in expected.iter().enumerate() {
        post_logs(
            http_address,
            &marked(body, 1_700_000_000_000_000_000 + index as u64),
        )
        .await;
    }
    reader.await.expect("query task");
    daemon.terminate().await;
}

fn marked(body: &str, time_unix_nano: u64) -> ExportLogsServiceRequest {
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

fn sql_body(sql: &str) -> String {
    json!({"sql": sql}).to_string()
}

fn bodies_of(document: &Value) -> Vec<String> {
    document["rows"]
        .as_array()
        .expect("rows")
        .iter()
        .map(|row| row["body"].as_str().expect("body string").to_owned())
        .collect()
}

fn assert_metrics(metrics: &Value, rows: u64) {
    let object = metrics.as_object().expect("metrics");
    for field in [
        "admission_wait_ns",
        "planning_ns",
        "execution_ns",
        "files_scanned",
        "files_pruned",
        "row_groups_pruned",
        "rows_returned",
        "memory_peak_bytes",
        "spill_bytes",
    ] {
        let text = object
            .get(field)
            .and_then(Value::as_str)
            .unwrap_or_else(|| panic!("{field} is not a decimal string in {metrics}"));
        assert!(
            !text.is_empty() && text.chars().all(|character| character.is_ascii_digit()),
            "{field}={text}"
        );
    }
    assert_eq!(
        object.get("rows_returned").and_then(Value::as_str),
        Some(rows.to_string()).as_deref()
    );
    assert_eq!(object.get("timed_out"), Some(&Value::Bool(false)));
    assert_eq!(object.get("cancelled"), Some(&Value::Bool(false)));
    assert_eq!(object.len(), 11);
}

async fn wait_for_bodies(address: std::net::SocketAddr, bearer: &str, expected: &[&str]) {
    let deadline = Instant::now() + TEST_TIMEOUT;
    let mut last = String::new();
    while Instant::now() < deadline {
        let response = post_query(
            address,
            Some(bearer),
            &sql_body("SELECT body FROM logs ORDER BY body"),
        )
        .await;
        last = response.body;
        if response.status == reqwest::StatusCode::OK {
            let document = serde_json::from_str::<Value>(&last).expect("query json");
            let bodies = bodies_of(&document);
            if bodies
                .iter()
                .map(String::as_str)
                .eq(expected.iter().copied())
            {
                return;
            }
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("rows did not become {expected:?}: {last}");
}

async fn wait_checkpoint(wal_directory: &Path, tenant: &str, sequence: u64) {
    let directory = tenant_wal_directory(wal_directory, tenant);
    let start = Instant::now();
    while start.elapsed() < TEST_TIMEOUT {
        if WalCheckpoint::load(&directory)
            .ok()
            .is_some_and(|checkpoint| checkpoint.cursor().next_sequence() == sequence)
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("checkpoint for {tenant} did not reach {sequence}");
}

async fn start_with_retries(config_path: &Path, wal_directory: &Path) -> (ListenAddrs, Observerd) {
    let mut last_error = String::from("no attempts");
    for _ in 0..START_ATTEMPTS {
        let listen = reserve_ports();
        write_config(config_path, wal_directory, listen);
        let mut daemon = Observerd::spawn(config_path);
        match timeout(TEST_TIMEOUT, daemon.wait_ready(listen.admin)).await {
            Ok(Ok(())) => return (listen, daemon),
            Ok(Err(error)) => last_error = error,
            Err(_) => last_error = "timed out waiting for first /ready".to_owned(),
        }
    }
    panic!("failed to start observerd after {START_ATTEMPTS} attempts: {last_error}");
}

async fn restart(config_path: &Path, listen: ListenAddrs) -> Observerd {
    let mut last_error = String::from("no attempts");
    for _ in 0..START_ATTEMPTS {
        let mut daemon = Observerd::spawn(config_path);
        match timeout(TEST_TIMEOUT, daemon.wait_ready(listen.admin)).await {
            Ok(Ok(())) => return daemon,
            Ok(Err(error)) => last_error = error,
            Err(_) => last_error = "timed out waiting for restart /ready".to_owned(),
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("failed to restart observerd after {START_ATTEMPTS} attempts: {last_error}");
}

async fn start(config: &Path, listen: ListenAddrs) -> Observerd {
    let mut daemon = Observerd::spawn(config);
    daemon.wait_ready(listen.admin).await.expect("ready");
    daemon
}
