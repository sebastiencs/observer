#![cfg(unix)]

mod support;

use std::time::{Duration, Instant};

use observer_protocol::otlp::{
    AnyValue, ExportLogsServiceRequest, LogRecord, ResourceLogs, ScopeLogs, any_value,
};
use serde_json::{Value, json};
use support::{
    BEARER, SECOND_BEARER, TENANT, TEST_TIMEOUT, assert_metrics, otlp, post_logs, post_logs_as,
    post_query, reserve_ports, restart_daemon, sql_body, start_daemon, start_ready,
    wait_checkpoint, wait_for_bodies, write_response_cap,
};
use tokio::time::timeout;

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
    let (listen, mut daemon) = start_daemon(&config_path, &wal_directory).await;

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
    let mut daemon = restart_daemon(&config_path, listen).await;
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
    let (listen, mut daemon) = start_daemon(&config_path, &wal_directory).await;

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
    let mut daemon = start_ready(&config_path, listen).await;
    post_logs(listen.http, &marked("too-big", 1)).await;
    wait_checkpoint(&wal_directory, TENANT, 1).await;
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
    let (listen, mut daemon) = start_daemon(&config_path, &wal_directory).await;
    let expected = concurrent_rows();
    let query_address = listen.query;
    let http_address = listen.http;
    let rows = expected.clone();
    let reader = tokio::spawn(async move {
        let deadline = Instant::now() + TEST_TIMEOUT;
        let mut last = String::new();
        while Instant::now() < deadline {
            let response = post_query(query_address, Some(BEARER), &sql_body(SNAPSHOT_SQL)).await;
            last = response.body;
            if response.status != reqwest::StatusCode::OK {
                tokio::time::sleep(Duration::from_millis(10)).await;
                continue;
            }
            let document = serde_json::from_str::<Value>(&last).expect("query json");
            assert_pinned_prefix(&document, &rows);
            if document["rows"]
                .as_array()
                .is_some_and(|seen| seen.len() == rows.len())
            {
                assert_metrics(
                    &document["metrics"],
                    u64::try_from(rows.len()).expect("len"),
                );
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("concurrent query never saw the full snapshot: {last}");
    });
    for request in concurrent_requests() {
        post_logs(http_address, &request).await;
    }
    reader.await.expect("query task");
    let evolved = post_query(listen.query, Some(BEARER), &sql_body(EVOLVED_SQL)).await;
    assert_eq!(evolved.status, reqwest::StatusCode::OK, "{}", evolved.body);
    let document = evolved.json();
    assert_eq!(document["rows"], evolved_rows());
    assert_metrics(
        &document["metrics"],
        u64::try_from(expected.len()).expect("len"),
    );
    daemon.terminate().await;
}

const SNAPSHOT_SQL: &str = "SELECT wal_sequence, record_index, body, log_kind_string FROM logs ORDER BY wal_sequence, record_index";
const EVOLVED_SQL: &str = "SELECT wal_sequence, record_index, body, log_extra_i64, log_region_string FROM logs ORDER BY wal_sequence, record_index";

fn concurrent_requests() -> [ExportLogsServiceRequest; 3] {
    [
        frame(0, &[("a0", "alpha"), ("a1", "alpha")], None, None),
        frame(1, &[("b0", "beta"), ("b1", "beta")], Some(1), None),
        frame(2, &[("c0", "gamma"), ("c1", "gamma")], Some(2), Some("eu")),
    ]
}

fn frame(
    sequence: u64,
    records: &[(&str, &str)],
    extra: Option<i64>,
    region: Option<&str>,
) -> ExportLogsServiceRequest {
    let mut log_records = Vec::new();
    for (index, (body, kind)) in records.iter().enumerate() {
        let mut attributes = vec![otlp::attribute("kind", otlp::string_value(kind))];
        if let Some(extra) = extra {
            attributes.push(otlp::attribute("extra", otlp::int_value(extra)));
        }
        if let Some(region) = region {
            attributes.push(otlp::attribute("region", otlp::string_value(region)));
        }
        log_records.push(LogRecord {
            time_unix_nano: 1_700_000_000_000_000_000 + sequence * 10 + index as u64,
            body: Some(otlp::string_value(body)),
            attributes,
            ..Default::default()
        });
    }
    otlp::logs(vec![otlp::resource(
        vec![],
        vec![otlp::scope(vec![], log_records)],
    )])
}

fn concurrent_rows() -> Vec<Value> {
    let mut rows = Vec::new();
    for (sequence, (bodies, kind)) in [
        (["a0", "a1"], "alpha"),
        (["b0", "b1"], "beta"),
        (["c0", "c1"], "gamma"),
    ]
    .into_iter()
    .enumerate()
    {
        for (index, body) in bodies.into_iter().enumerate() {
            rows.push(json!({
                "wal_sequence": sequence.to_string(),
                "record_index": index,
                "body": body,
                "log_kind_string": kind,
            }));
        }
    }
    rows
}

fn evolved_rows() -> Value {
    json!([
        {"wal_sequence": "0", "record_index": 0, "body": "a0", "log_extra_i64": null, "log_region_string": null},
        {"wal_sequence": "0", "record_index": 1, "body": "a1", "log_extra_i64": null, "log_region_string": null},
        {"wal_sequence": "1", "record_index": 0, "body": "b0", "log_extra_i64": "1", "log_region_string": null},
        {"wal_sequence": "1", "record_index": 1, "body": "b1", "log_extra_i64": "1", "log_region_string": null},
        {"wal_sequence": "2", "record_index": 0, "body": "c0", "log_extra_i64": "2", "log_region_string": "eu"},
        {"wal_sequence": "2", "record_index": 1, "body": "c1", "log_extra_i64": "2", "log_region_string": "eu"}
    ])
}

fn assert_pinned_prefix(document: &Value, expected: &[Value]) {
    let rows = document["rows"].as_array().expect("rows");
    assert!(
        rows.len() <= expected.len() && rows.len().is_multiple_of(2),
        "snapshot split a frame: {rows:?}"
    );
    assert_eq!(&rows[..], &expected[..rows.len()]);
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
