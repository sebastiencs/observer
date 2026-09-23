#![cfg(unix)]

mod support;

use observer_protocol::otlp::ExportLogsServiceResponse;
use prost::Message;
use reqwest::header::CONTENT_TYPE;
use serde_json::json;
use support::{
    BEARER, HttpBytes, QueryCase, TEST_TIMEOUT, assert_cases_across_restart, assert_query_cases,
    otlp, post_logs, post_logs_raw, start_daemon,
};
use tokio::time::timeout;

#[tokio::test]
async fn one_log_round_trips_before_and_after_restart() {
    timeout(TEST_TIMEOUT * 3, round_trip())
        .await
        .expect("log round trip timed out");
}

#[tokio::test]
async fn accepted_content_types_ingest_and_an_empty_request_adds_no_rows() {
    timeout(TEST_TIMEOUT * 3, accepted_ingest())
        .await
        .expect("accepted ingest scenario timed out");
}

async fn accepted_ingest() {
    let root = tempfile::tempdir().expect("tempdir");
    let wal_directory = root.path().join("wal");
    let config_path = root.path().join("observerd.toml");
    let (listen, mut daemon) = start_daemon(&config_path, &wal_directory).await;

    let empty = post_logs_raw(
        listen.http,
        Some(BEARER),
        Some("application/x-protobuf"),
        otlp::logs(vec![]).encode_to_vec(),
    )
    .await;
    assert_export_ok(&empty);

    let accepted = [
        ("application/x-protobuf", "plain", 1_700_000_000_000_000_001),
        ("APPLICATION/X-PROTOBUF", "cased", 1_700_000_000_000_000_002),
        (
            "application/x-protobuf; charset=binary",
            "parameter",
            1_700_000_000_000_000_003,
        ),
    ];
    for (content_type, body, time) in accepted {
        let response = post_logs_raw(
            listen.http,
            Some(BEARER),
            Some(content_type),
            one_log(body, time).encode_to_vec(),
        )
        .await;
        assert_export_ok(&response);
    }

    assert_query_cases(
        listen.query,
        BEARER,
        &[QueryCase {
            name: "accepted content types",
            sql: "SELECT body, event_time_unix_nano, wal_sequence, record_index FROM logs ORDER BY wal_sequence, record_index",
            schema: json!([
                {"name": "body", "type": "Utf8", "nullable": true},
                {"name": "event_time_unix_nano", "type": "UInt64", "nullable": false},
                {"name": "wal_sequence", "type": "UInt64", "nullable": false},
                {"name": "record_index", "type": "UInt32", "nullable": false}
            ]),
            rows: json!([
                {"body": "plain", "event_time_unix_nano": "1700000000000000001", "wal_sequence": "1", "record_index": 0},
                {"body": "cased", "event_time_unix_nano": "1700000000000000002", "wal_sequence": "2", "record_index": 0},
                {"body": "parameter", "event_time_unix_nano": "1700000000000000003", "wal_sequence": "3", "record_index": 0}
            ]),
        }],
    )
    .await;
    daemon.terminate().await;
}

fn one_log(body: &str, time_unix_nano: u64) -> observer_protocol::otlp::ExportLogsServiceRequest {
    otlp::logs(vec![otlp::resource(
        vec![],
        vec![otlp::scope(
            vec![],
            vec![otlp::text_record(body, time_unix_nano)],
        )],
    )])
}

fn assert_export_ok(response: &HttpBytes) {
    assert_eq!(
        response.status,
        reqwest::StatusCode::OK,
        "{}",
        response.text()
    );
    assert_eq!(
        response
            .headers
            .get(CONTENT_TYPE)
            .and_then(|value| value.to_str().ok()),
        Some("application/x-protobuf")
    );
    let decoded = ExportLogsServiceResponse::decode(response.body.as_slice())
        .expect("decode export response");
    assert!(decoded.partial_success.is_none());
}

async fn round_trip() {
    let root = tempfile::tempdir().expect("tempdir");
    let wal_directory = root.path().join("wal");
    let config_path = root.path().join("observerd.toml");
    let (listen, daemon) = start_daemon(&config_path, &wal_directory).await;
    post_logs(
        listen.http,
        &otlp::logs(vec![otlp::resource(
            vec![],
            vec![otlp::scope(
                vec![],
                vec![otlp::text_record("alpha", 1_700_000_000_000_000_000)],
            )],
        )]),
    )
    .await;
    let cases = [QueryCase {
        name: "alpha",
        sql: "SELECT body, event_time_unix_nano, wal_sequence, record_index FROM logs ORDER BY wal_sequence, record_index",
        schema: json!([
            {"name": "body", "type": "Utf8", "nullable": true},
            {"name": "event_time_unix_nano", "type": "UInt64", "nullable": false},
            {"name": "wal_sequence", "type": "UInt64", "nullable": false},
            {"name": "record_index", "type": "UInt32", "nullable": false}
        ]),
        rows: json!([{
            "body": "alpha",
            "event_time_unix_nano": "1700000000000000000",
            "wal_sequence": "0",
            "record_index": 0
        }]),
    }];
    let mut daemon =
        assert_cases_across_restart(&config_path, listen, daemon, BEARER, &cases).await;
    daemon.terminate().await;
}
