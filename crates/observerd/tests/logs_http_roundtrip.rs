#![cfg(unix)]

mod support;

use serde_json::json;
use support::{
    BEARER, QueryCase, TEST_TIMEOUT, assert_cases_across_restart, otlp, post_logs, start_daemon,
};
use tokio::time::timeout;

#[tokio::test]
async fn one_log_round_trips_before_and_after_restart() {
    timeout(TEST_TIMEOUT * 3, round_trip())
        .await
        .expect("log round trip timed out");
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
