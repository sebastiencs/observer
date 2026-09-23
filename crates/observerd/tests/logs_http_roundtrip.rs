#![cfg(unix)]

mod support;

use base64::{Engine as _, engine::general_purpose::STANDARD};
use observer_protocol::otlp::{
    AnyValue, ExportLogsServiceRequest, ExportLogsServiceResponse, LogRecord, any_value,
};
use prost::Message;
use reqwest::header::CONTENT_TYPE;
use serde_json::{Value, json};
use support::{
    BEARER, HttpBytes, QueryCase, TENANT, TEST_TIMEOUT, assert_cases_across_restart,
    assert_query_cases, otlp, poll_query, post_logs, post_logs_raw, start_daemon,
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

const HOUR_START: u64 = 1_699_999_200_000_000_000;
const HOUR_END: u64 = 1_700_002_799_999_999_999;
const NEXT_HOUR: u64 = 1_700_002_800_000_000_000;
const HOUR_START_TEXT: &str = "1699999200000000000";
const HOUR_END_TEXT: &str = "1700002799999999999";
const NEXT_HOUR_TEXT: &str = "1700002800000000000";
const RESOURCE_JSON: &str = r#"{"service.name":"api","tenant.id":"attacker","zone":"eu"}"#;
const SCOPE_JSON: &str = r#"{"lib":"probe"}"#;
const LOG_JSON: &str = r#"{"msg":"a\"b\\c\n日","n":3,"ok":true,"raw":{"$bytes":"aGk="},"tags":[1,"x",null,{"$bytes":"aGk="}]}"#;
const CORE_SQL: &str = "SELECT schema_version, tenant_id, wal_sequence, record_index, time_unix_nano, observed_time_unix_nano, received_time_unix_nano, event_time_unix_nano, severity_number, severity_text, body, trace_id, span_id, service_name, resource_attributes, scope_attributes, log_attributes FROM logs ORDER BY wal_sequence, record_index";

#[tokio::test]
async fn core_log_fields_keep_source_order_across_restart() {
    timeout(TEST_TIMEOUT * 4, core_fields())
        .await
        .expect("core log projection timed out");
}

async fn core_fields() {
    let root = tempfile::tempdir().expect("tempdir");
    let wal_directory = root.path().join("wal");
    let config_path = root.path().join("observerd.toml");
    let (listen, daemon) = start_daemon(&config_path, &wal_directory).await;
    post_logs(listen.http, &core_request()).await;
    post_logs(listen.http, &body_request()).await;

    let document = poll_query(listen.query, BEARER, CORE_SQL, |document| {
        document["rows"]
            .as_array()
            .is_some_and(|rows| rows.len() == 16)
    })
    .await;
    let rows = document["rows"].as_array().expect("rows");
    let first_received = frame_received(rows, 0..5);
    let second_received = frame_received(rows, 5..16);
    let cases = [QueryCase {
        name: "core log fields",
        sql: CORE_SQL,
        schema: core_schema(),
        rows: expected_rows(&first_received, &second_received),
    }];
    let mut daemon =
        assert_cases_across_restart(&config_path, listen, daemon, BEARER, &cases).await;
    daemon.terminate().await;
}

fn core_request() -> ExportLogsServiceRequest {
    let rich = LogRecord {
        time_unix_nano: HOUR_END,
        observed_time_unix_nano: HOUR_START,
        severity_number: 9,
        severity_text: "INFO".to_owned(),
        body: Some(otlp::string_value("say \"hi\"\\日\n")),
        attributes: vec![
            otlp::attribute(
                "tags",
                otlp::array_value(vec![
                    otlp::int_value(1),
                    otlp::string_value("x"),
                    AnyValue { value: None },
                    otlp::bytes_value(b"hi"),
                ]),
            ),
            otlp::attribute("ok", otlp::bool_value(true)),
            otlp::attribute("msg", otlp::string_value("a\"b\\c\n日")),
            otlp::attribute("n", otlp::int_value(3)),
            otlp::attribute("raw", otlp::bytes_value(b"hi")),
        ],
        dropped_attributes_count: 9,
        flags: 1,
        trace_id: vec![0x11; 16],
        span_id: vec![0x22; 8],
        event_name: "ignored.event".to_owned(),
    };
    let next_hour = LogRecord {
        time_unix_nano: NEXT_HOUR,
        severity_number: 13,
        severity_text: "WARN".to_owned(),
        body: Some(otlp::string_value("later-hour")),
        ..Default::default()
    };
    let observed_only = LogRecord {
        observed_time_unix_nano: HOUR_START,
        body: Some(otlp::string_value("")),
        trace_id: vec![0; 16],
        span_id: vec![0; 8],
        ..Default::default()
    };
    let received_fallback = LogRecord {
        severity_number: 1,
        severity_text: " ".to_owned(),
        span_id: vec![0x22; 7],
        ..Default::default()
    };
    let invalid_ids = LogRecord {
        time_unix_nano: NEXT_HOUR,
        body: Some(AnyValue {
            value: Some(any_value::Value::StringValueStrindex(7)),
        }),
        attributes: vec![otlp::attribute("missing", AnyValue { value: None })],
        trace_id: vec![0x11; 17],
        span_id: vec![0x22; 9],
        ..Default::default()
    };

    let mut primary_scope = otlp::scope(
        vec![otlp::attribute("lib", otlp::string_value("probe"))],
        vec![rich, next_hour],
    );
    primary_scope.schema_url = "https://ignored.example/scope".to_owned();
    let scope = primary_scope.scope.as_mut().expect("scope");
    scope.name = "ignored-scope".to_owned();
    scope.version = "9".to_owned();
    scope.dropped_attributes_count = 4;
    let quiet_scope = otlp::scope(vec![], vec![observed_only]);

    let mut primary = otlp::resource(
        vec![
            otlp::attribute("zone", otlp::string_value("eu")),
            otlp::attribute("service.name", otlp::string_value("api")),
            otlp::attribute("tenant.id", otlp::string_value("attacker")),
        ],
        vec![primary_scope, quiet_scope],
    );
    primary.schema_url = "https://ignored.example/resource".to_owned();
    primary
        .resource
        .as_mut()
        .expect("resource")
        .dropped_attributes_count = 3;

    let secondary = otlp::resource(
        vec![],
        vec![otlp::scope(vec![], vec![received_fallback, invalid_ids])],
    );
    otlp::logs(vec![primary, secondary])
}

fn body_request() -> ExportLogsServiceRequest {
    let records = vec![
        timed(
            Some(otlp::string_value(r#"{"not":"canonical"}"#)),
            HOUR_START,
        ),
        timed(Some(otlp::int_value(-2)), HOUR_START),
        timed(Some(otlp::bool_value(true)), HOUR_START),
        timed(Some(otlp::bytes_value(b"hi")), HOUR_START),
        timed(Some(otlp::bytes_value(b"")), HOUR_START),
        timed(
            Some(otlp::array_value(vec![
                otlp::int_value(1),
                otlp::string_value("x"),
                AnyValue { value: None },
                otlp::bytes_value(b"hi"),
            ])),
            HOUR_START,
        ),
        timed(
            Some(otlp::kv_value(vec![
                otlp::attribute("z", otlp::bool_value(true)),
                otlp::attribute("a", otlp::int_value(1)),
            ])),
            HOUR_START,
        ),
        timed(Some(otlp::double_value(1.5)), HOUR_START),
        timed(Some(otlp::double_value(f64::NAN)), HOUR_START),
        timed(Some(otlp::double_value(f64::INFINITY)), HOUR_START),
        timed(Some(otlp::double_value(f64::NEG_INFINITY)), HOUR_START),
    ];
    otlp::logs(vec![otlp::resource(
        vec![],
        vec![otlp::scope(vec![], records)],
    )])
}

fn timed(body: Option<AnyValue>, time_unix_nano: u64) -> LogRecord {
    LogRecord {
        time_unix_nano,
        body,
        ..Default::default()
    }
}

fn frame_received(rows: &[Value], indexes: std::ops::Range<usize>) -> String {
    let received = timestamp_text(&rows[indexes.start]["received_time_unix_nano"]);
    for row in &rows[indexes] {
        assert_eq!(timestamp_text(&row["received_time_unix_nano"]), received);
    }
    received
}

fn timestamp_text(value: &Value) -> String {
    let text = value
        .as_str()
        .unwrap_or_else(|| panic!("timestamp is not a decimal string: {value}"));
    assert!(
        !text.is_empty()
            && !text.starts_with('0')
            && text.chars().all(|character| character.is_ascii_digit()),
        "{text}"
    );
    text.to_owned()
}

fn core_schema() -> Value {
    json!([
        {"name": "schema_version", "type": "UInt16", "nullable": false},
        {"name": "tenant_id", "type": "Utf8", "nullable": false},
        {"name": "wal_sequence", "type": "UInt64", "nullable": false},
        {"name": "record_index", "type": "UInt32", "nullable": false},
        {"name": "time_unix_nano", "type": "UInt64", "nullable": true},
        {"name": "observed_time_unix_nano", "type": "UInt64", "nullable": true},
        {"name": "received_time_unix_nano", "type": "UInt64", "nullable": false},
        {"name": "event_time_unix_nano", "type": "UInt64", "nullable": false},
        {"name": "severity_number", "type": "Int32", "nullable": true},
        {"name": "severity_text", "type": "Utf8", "nullable": true},
        {"name": "body", "type": "Utf8", "nullable": true},
        {"name": "trace_id", "type": "FixedSizeBinary(16)", "nullable": true},
        {"name": "span_id", "type": "FixedSizeBinary(8)", "nullable": true},
        {"name": "service_name", "type": "Utf8", "nullable": true},
        {"name": "resource_attributes", "type": "Utf8", "nullable": false},
        {"name": "scope_attributes", "type": "Utf8", "nullable": false},
        {"name": "log_attributes", "type": "Utf8", "nullable": false}
    ])
}

struct Expected<'a> {
    sequence: &'a str,
    index: u32,
    time: Option<&'a str>,
    observed: Option<&'a str>,
    received: &'a str,
    event: &'a str,
    severity_number: Option<i32>,
    severity_text: Option<&'a str>,
    body: Option<&'a str>,
    trace_id: Option<&'a str>,
    span_id: Option<&'a str>,
    service_name: Option<&'a str>,
    resource_attributes: &'a str,
    scope_attributes: &'a str,
    log_attributes: &'a str,
}

fn expected_rows(first_received: &str, second_received: &str) -> Value {
    let trace_id = STANDARD.encode([0x11; 16]);
    let span_id = STANDARD.encode([0x22; 8]);
    let rows = [
        Expected {
            sequence: "0",
            index: 0,
            time: Some(HOUR_END_TEXT),
            observed: Some(HOUR_START_TEXT),
            received: first_received,
            event: HOUR_END_TEXT,
            severity_number: Some(9),
            severity_text: Some("INFO"),
            body: Some("say \"hi\"\\日\n"),
            trace_id: Some(&trace_id),
            span_id: Some(&span_id),
            service_name: Some("api"),
            resource_attributes: RESOURCE_JSON,
            scope_attributes: SCOPE_JSON,
            log_attributes: LOG_JSON,
        },
        Expected {
            sequence: "0",
            index: 1,
            time: Some(NEXT_HOUR_TEXT),
            observed: None,
            received: first_received,
            event: NEXT_HOUR_TEXT,
            severity_number: Some(13),
            severity_text: Some("WARN"),
            body: Some("later-hour"),
            trace_id: None,
            span_id: None,
            service_name: Some("api"),
            resource_attributes: RESOURCE_JSON,
            scope_attributes: SCOPE_JSON,
            log_attributes: "{}",
        },
        Expected {
            sequence: "0",
            index: 2,
            time: None,
            observed: Some(HOUR_START_TEXT),
            received: first_received,
            event: HOUR_START_TEXT,
            severity_number: None,
            severity_text: None,
            body: Some(""),
            trace_id: None,
            span_id: None,
            service_name: Some("api"),
            resource_attributes: RESOURCE_JSON,
            scope_attributes: "{}",
            log_attributes: "{}",
        },
        Expected {
            sequence: "0",
            index: 3,
            time: None,
            observed: None,
            received: first_received,
            event: first_received,
            severity_number: Some(1),
            severity_text: Some(" "),
            body: None,
            trace_id: None,
            span_id: None,
            service_name: None,
            resource_attributes: "{}",
            scope_attributes: "{}",
            log_attributes: "{}",
        },
        Expected {
            sequence: "0",
            index: 4,
            time: Some(NEXT_HOUR_TEXT),
            observed: None,
            received: first_received,
            event: NEXT_HOUR_TEXT,
            severity_number: None,
            severity_text: None,
            body: None,
            trace_id: None,
            span_id: None,
            service_name: None,
            resource_attributes: "{}",
            scope_attributes: "{}",
            log_attributes: r#"{"missing":null}"#,
        },
    ];
    let mut values = rows.map(expected_row).to_vec();
    for (index, body) in [
        r#"{"not":"canonical"}"#,
        "-2",
        "true",
        r#"{"$bytes":"aGk="}"#,
        r#"{"$bytes":""}"#,
        r#"[1,"x",null,{"$bytes":"aGk="}]"#,
        r#"{"a":1,"z":true}"#,
        "1.5",
        r#"{"$float":"NaN"}"#,
        r#"{"$float":"Infinity"}"#,
        r#"{"$float":"-Infinity"}"#,
    ]
    .into_iter()
    .enumerate()
    {
        values.push(expected_row(Expected {
            sequence: "1",
            index: u32::try_from(index).expect("index"),
            time: Some(HOUR_START_TEXT),
            observed: None,
            received: second_received,
            event: HOUR_START_TEXT,
            severity_number: None,
            severity_text: None,
            body: Some(body),
            trace_id: None,
            span_id: None,
            service_name: None,
            resource_attributes: "{}",
            scope_attributes: "{}",
            log_attributes: "{}",
        }));
    }
    Value::Array(values)
}

fn expected_row(row: Expected<'_>) -> Value {
    json!({
        "schema_version": 1,
        "tenant_id": TENANT,
        "wal_sequence": row.sequence,
        "record_index": row.index,
        "time_unix_nano": row.time,
        "observed_time_unix_nano": row.observed,
        "received_time_unix_nano": row.received,
        "event_time_unix_nano": row.event,
        "severity_number": row.severity_number,
        "severity_text": row.severity_text,
        "body": row.body,
        "trace_id": row.trace_id,
        "span_id": row.span_id,
        "service_name": row.service_name,
        "resource_attributes": row.resource_attributes,
        "scope_attributes": row.scope_attributes,
        "log_attributes": row.log_attributes,
    })
}
