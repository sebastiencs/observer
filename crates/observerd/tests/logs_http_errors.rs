#![cfg(unix)]

mod support;

use std::net::SocketAddr;

use observer_protocol::otlp::ExportLogsServiceRequest;
use prost::Message;
use reqwest::StatusCode;
use serde_json::json;
use support::{
    BEARER, HttpBytes, QueryCase, TENANT, TEST_TIMEOUT, assert_query_cases, frames_on_disk, otlp,
    post_logs_raw, start_daemon,
};
use tokio::time::timeout;

/// `observerd` rejects `POST /v1/logs` bodies above this size.
const INGEST_BODY_LIMIT: usize = 16 * 1024 * 1024;
const REJECTED: &str = "rejected-marker";

#[tokio::test]
async fn rejected_log_posts_do_not_become_queryable() {
    timeout(TEST_TIMEOUT * 6, rejected_posts())
        .await
        .expect("rejected ingest scenario timed out");
}

async fn rejected_posts() {
    let root = tempfile::tempdir().expect("tempdir");
    let wal_directory = root.path().join("wal");
    let config_path = root.path().join("observerd.toml");
    let (listen, mut daemon) = start_daemon(&config_path, &wal_directory).await;
    let payload = one_log(REJECTED, 1).encode_to_vec();

    for bearer in [
        None,
        Some(""),
        Some("secret-a"),
        Some("Basic secret-a"),
        Some("Bearer"),
        Some("Bearer secret-a extra"),
        Some("Bearer unknown-token"),
    ] {
        let response = post_logs_raw(
            listen.http,
            bearer,
            Some("application/x-protobuf"),
            payload.clone(),
        )
        .await;
        assert_unauthenticated(&response);
    }

    let before_body = post_logs_raw(
        listen.http,
        Some("Bearer unknown-token"),
        Some("application/json"),
        format!("{REJECTED}-not-protobuf").into_bytes(),
    )
    .await;
    assert_unauthenticated(&before_body);

    for content_type in [None, Some("application/json"), Some("application/protobuf")] {
        let response =
            post_logs_raw(listen.http, Some(BEARER), content_type, payload.clone()).await;
        assert_unsupported(&response);
    }

    for body in [
        vec![0xff, 0x00],
        format!("not-protobuf-{REJECTED}").into_bytes(),
    ] {
        let response = post_logs_raw(
            listen.http,
            Some(BEARER),
            Some("application/x-protobuf"),
            body,
        )
        .await;
        assert_invalid_protobuf(&response);
    }

    let method = exchange(
        listen.http,
        reqwest::Method::GET,
        "/v1/logs",
        Some(BEARER),
        Some("application/x-protobuf"),
        payload.clone(),
    )
    .await;
    assert_eq!(method.status, StatusCode::METHOD_NOT_ALLOWED);
    assert!(!method.text().contains(REJECTED));

    let path = exchange(
        listen.http,
        reqwest::Method::POST,
        "/v1/traces",
        Some(BEARER),
        Some("application/x-protobuf"),
        payload.clone(),
    )
    .await;
    assert_eq!(path.status, StatusCode::NOT_FOUND);
    assert!(!path.text().contains(REJECTED));

    let mut at_limit = b"oversized-marker".to_vec();
    at_limit.resize(INGEST_BODY_LIMIT, 0xff);
    let at_limit_response = post_logs_raw(
        listen.http,
        Some(BEARER),
        Some("application/x-protobuf"),
        at_limit,
    )
    .await;
    assert_invalid_protobuf(&at_limit_response);
    assert!(!at_limit_response.text().contains("oversized-marker"));

    let mut over_limit = b"oversized-marker".to_vec();
    over_limit.resize(INGEST_BODY_LIMIT + 1, 0xff);
    let over_limit_response = post_logs_raw(
        listen.http,
        Some(BEARER),
        Some("application/x-protobuf"),
        over_limit,
    )
    .await;
    assert_eq!(over_limit_response.status, StatusCode::PAYLOAD_TOO_LARGE);
    assert!(!over_limit_response.text().contains("oversized-marker"));
    assert!(!over_limit_response.text().contains(REJECTED));

    let sentinel = one_log("sentinel", 1_700_000_000_000_000_000);
    let accepted = post_logs_raw(
        listen.http,
        Some(BEARER),
        Some("application/x-protobuf"),
        sentinel.encode_to_vec(),
    )
    .await;
    assert_eq!(accepted.status, StatusCode::OK, "{}", accepted.text());

    let frames = frames_on_disk(&wal_directory, TENANT);
    assert_eq!(frames.len(), 1, "rejected posts must not reach the WAL");
    assert_eq!(
        ExportLogsServiceRequest::decode(frames[0].payload.clone()).expect("sentinel payload"),
        sentinel
    );

    assert_query_cases(
        listen.query,
        BEARER,
        &[QueryCase {
            name: "only the accepted row",
            sql: "SELECT body FROM logs ORDER BY wal_sequence, record_index",
            schema: json!([{"name": "body", "type": "Utf8", "nullable": true}]),
            rows: json!([{"body": "sentinel"}]),
        }],
    )
    .await;
    daemon.terminate().await;
}

fn one_log(body: &str, time_unix_nano: u64) -> ExportLogsServiceRequest {
    otlp::logs(vec![otlp::resource(
        vec![],
        vec![otlp::scope(
            vec![],
            vec![otlp::text_record(body, time_unix_nano)],
        )],
    )])
}

fn assert_unauthenticated(response: &HttpBytes) {
    assert_eq!(
        response.status,
        StatusCode::UNAUTHORIZED,
        "{}",
        response.text()
    );
    assert_eq!(response.text(), "unauthenticated");
    assert!(!response.text().contains("secret-a"));
    assert!(!response.text().contains("unknown-token"));
    assert!(!response.text().contains(REJECTED));
}

fn assert_unsupported(response: &HttpBytes) {
    assert_eq!(
        response.status,
        StatusCode::UNSUPPORTED_MEDIA_TYPE,
        "{}",
        response.text()
    );
    assert_eq!(response.text(), "unsupported content type");
    assert!(!response.text().contains(REJECTED));
}

fn assert_invalid_protobuf(response: &HttpBytes) {
    assert_eq!(
        response.status,
        StatusCode::BAD_REQUEST,
        "{}",
        response.text()
    );
    assert_eq!(response.text(), "invalid protobuf");
    assert!(!response.text().contains(REJECTED));
}

async fn exchange(
    address: SocketAddr,
    method: reqwest::Method,
    path: &str,
    bearer: Option<&str>,
    content_type: Option<&str>,
    body: Vec<u8>,
) -> HttpBytes {
    let client = reqwest::Client::new();
    let mut request = client
        .request(method, format!("http://{address}{path}"))
        .body(body);
    if let Some(content_type) = content_type {
        request = request.header(reqwest::header::CONTENT_TYPE, content_type);
    }
    if let Some(bearer) = bearer {
        request = request.header(reqwest::header::AUTHORIZATION, bearer);
    }
    let response = request.send().await.expect("http exchange");
    let status = response.status();
    let headers = response.headers().clone();
    let body = response.bytes().await.expect("response body").to_vec();
    HttpBytes {
        status,
        headers,
        body,
    }
}
