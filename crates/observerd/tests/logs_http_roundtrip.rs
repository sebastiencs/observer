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
    BEARER, HttpBytes, QueryCase, SECOND_BEARER, StorageLimits, TENANT, TEST_TIMEOUT,
    assert_cases_across_restart, assert_query_cases, otlp, poll_query, post_logs, post_logs_as,
    post_logs_raw, post_query, reserve_ports, sql_body, start_daemon, start_ready, wait_checkpoint,
    write_storage,
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

#[tokio::test]
async fn dynamic_attributes_survive_new_columns_hours_and_restart() {
    timeout(TEST_TIMEOUT * 4, dynamic_attributes())
        .await
        .expect("dynamic attribute projection timed out");
}

#[tokio::test]
async fn deep_maps_and_columns_past_the_cap_stay_in_json() {
    timeout(TEST_TIMEOUT * 4, limited_dynamic_columns())
        .await
        .expect("dynamic limit projection timed out");
}

#[tokio::test]
async fn dynamic_attributes_match_while_active_and_after_restart() {
    timeout(TEST_TIMEOUT * 4, active_dynamic_attributes())
        .await
        .expect("active dynamic projection timed out");
}

#[tokio::test]
async fn one_query_reads_published_frozen_and_active_hours_once() {
    timeout(TEST_TIMEOUT * 4, mixed_generations())
        .await
        .expect("mixed generation query timed out");
}

#[tokio::test]
async fn dynamic_columns_stay_inside_the_token_tenant() {
    timeout(TEST_TIMEOUT * 4, tenant_dynamic_columns())
        .await
        .expect("dynamic tenant isolation timed out");
}

async fn active_dynamic_attributes() {
    let root = tempfile::tempdir().expect("tempdir");
    let config_path = root.path().join("observerd.toml");
    let (listen, daemon) = start_daemon(&config_path, &root.path().join("wal")).await;
    post_logs(listen.http, &dynamic_request()).await;
    post_logs(listen.http, &later_column_request()).await;
    let mut daemon =
        assert_cases_across_restart(&config_path, listen, daemon, BEARER, &dynamic_cases()).await;
    daemon.terminate().await;
}

async fn dynamic_attributes() {
    let root = tempfile::tempdir().expect("tempdir");
    let wal_directory = root.path().join("wal");
    let config_path = root.path().join("observerd.toml");
    let listen = reserve_ports();
    write_storage(
        &config_path,
        &wal_directory,
        listen,
        &StorageLimits {
            max_rows: 0,
            ..StorageLimits::default()
        },
    );
    let daemon = start_ready(&config_path, listen).await;
    post_logs(listen.http, &dynamic_request()).await;
    wait_checkpoint(&wal_directory, TENANT, 1).await;
    post_logs(listen.http, &later_column_request()).await;
    wait_checkpoint(&wal_directory, TENANT, 2).await;

    let mut daemon =
        assert_cases_across_restart(&config_path, listen, daemon, BEARER, &dynamic_cases()).await;
    daemon.terminate().await;
}

fn dynamic_cases() -> [QueryCase; 1] {
    let dotted = collision_name(&["http.status"]);
    let nested = collision_name(&["http", "status"]);
    let sql = format!(
        "SELECT wal_sequence, record_index, service_name, resource_attributes, scope_attributes, log_attributes, resource_service_name_string, resource_service_name_i64, scope_lib_string, log_host_string, log_ok_bool, log_status_i64, log_status_string, log_latency_f64, log_nan_f64, log_payload_bytes, log_items_json, log_region_string, log_env_string, {dotted}, {nested} FROM logs ORDER BY wal_sequence, record_index"
    );
    [QueryCase {
        name: "dynamic attributes",
        sql: Box::leak(sql.into_boxed_str()),
        schema: dynamic_schema(&dotted, &nested),
        rows: dynamic_rows(&dotted, &nested),
    }]
}

async fn mixed_generations() {
    let root = tempfile::tempdir().expect("tempdir");
    let wal_directory = root.path().join("wal");
    let config_path = root.path().join("observerd.toml");
    let listen = reserve_ports();
    write_storage(
        &config_path,
        &wal_directory,
        listen,
        &StorageLimits {
            max_rows: 1,
            ..StorageLimits::default()
        },
    );
    let daemon = start_ready(&config_path, listen).await;
    let sql = "SELECT body, event_time_unix_nano, wal_sequence, log_marker_string FROM logs ORDER BY wal_sequence, record_index";
    post_logs(listen.http, &marked_hour("published-a", HOUR_START)).await;
    post_logs(listen.http, &marked_hour("published-b", HOUR_END)).await;
    wait_checkpoint(&wal_directory, TENANT, 2).await;
    post_logs(listen.http, &marked_hour("active-a", NEXT_HOUR)).await;
    wait_for_rows(listen.query, sql, 3).await;
    assert_eq!(checkpoint_sequence(&wal_directory, TENANT), 2);
    post_logs(listen.http, &marked_hour("sealed-b", HOUR_START)).await;
    post_logs(listen.http, &marked_hour("active-c", HOUR_END)).await;
    let expected = json!([
        {"body": "published-a", "event_time_unix_nano": HOUR_START_TEXT, "wal_sequence": "0", "log_marker_string": "published-a"},
        {"body": "published-b", "event_time_unix_nano": HOUR_END_TEXT, "wal_sequence": "1", "log_marker_string": "published-b"},
        {"body": "active-a", "event_time_unix_nano": NEXT_HOUR_TEXT, "wal_sequence": "2", "log_marker_string": "active-a"},
        {"body": "sealed-b", "event_time_unix_nano": HOUR_START_TEXT, "wal_sequence": "3", "log_marker_string": "sealed-b"},
        {"body": "active-c", "event_time_unix_nano": HOUR_END_TEXT, "wal_sequence": "4", "log_marker_string": "active-c"}
    ]);
    let document = poll_query(listen.query, BEARER, sql, |document| {
        document["rows"] == expected
    })
    .await;
    assert_eq!(document["rows"], expected);
    let cases = [QueryCase {
        name: "mixed generations",
        sql,
        schema: json!([
            {"name": "body", "type": "Utf8", "nullable": true},
            {"name": "event_time_unix_nano", "type": "UInt64", "nullable": false},
            {"name": "wal_sequence", "type": "UInt64", "nullable": false},
            {"name": "log_marker_string", "type": "Utf8", "nullable": true}
        ]),
        rows: expected,
    }];
    let mut daemon =
        assert_cases_across_restart(&config_path, listen, daemon, BEARER, &cases).await;
    daemon.terminate().await;
}

fn marked_hour(body: &str, time_unix_nano: u64) -> ExportLogsServiceRequest {
    otlp::logs(vec![otlp::resource(
        vec![],
        vec![otlp::scope(
            vec![],
            vec![LogRecord {
                time_unix_nano,
                body: Some(otlp::string_value(body)),
                attributes: vec![otlp::attribute("marker", otlp::string_value(body))],
                ..Default::default()
            }],
        )],
    )])
}

async fn wait_for_rows(address: std::net::SocketAddr, sql: &str, count: usize) {
    poll_query(address, BEARER, sql, |document| {
        document["rows"]
            .as_array()
            .is_some_and(|rows| rows.len() == count)
    })
    .await;
}

fn checkpoint_sequence(wal_directory: &std::path::Path, tenant: &str) -> u64 {
    observer_wal::WalCheckpoint::load(observer_wal::tenant_wal_directory(wal_directory, tenant))
        .map(|checkpoint| checkpoint.cursor().next_sequence())
        .unwrap_or(0)
}

async fn tenant_dynamic_columns() {
    let root = tempfile::tempdir().expect("tempdir");
    let config_path = root.path().join("observerd.toml");
    let (listen, daemon) = start_daemon(&config_path, &root.path().join("wal")).await;
    post_logs(listen.http, &tenant_log("alpha-row", "alpha", None)).await;
    post_logs_as(
        listen.http,
        SECOND_BEARER,
        &tenant_log("beta-row", "beta", Some("eu")),
    )
    .await;
    let own = [QueryCase {
        name: "tenant a dynamic columns",
        sql: "SELECT body, log_host_string, log_attributes FROM logs ORDER BY wal_sequence",
        schema: json!([
            {"name": "body", "type": "Utf8", "nullable": true},
            {"name": "log_host_string", "type": "Utf8", "nullable": true},
            {"name": "log_attributes", "type": "Utf8", "nullable": false}
        ]),
        rows: json!([{
            "body": "alpha-row",
            "log_host_string": "alpha",
            "log_attributes": r#"{"host":"alpha"}"#
        }]),
    }];
    let other = [QueryCase {
        name: "tenant b dynamic columns",
        sql: "SELECT body, log_host_string, log_region_string, log_attributes FROM logs ORDER BY wal_sequence",
        schema: json!([
            {"name": "body", "type": "Utf8", "nullable": true},
            {"name": "log_host_string", "type": "Utf8", "nullable": true},
            {"name": "log_region_string", "type": "Utf8", "nullable": true},
            {"name": "log_attributes", "type": "Utf8", "nullable": false}
        ]),
        rows: json!([{
            "body": "beta-row",
            "log_host_string": "beta",
            "log_region_string": "eu",
            "log_attributes": r#"{"host":"beta","region":"eu"}"#
        }]),
    }];
    assert_query_cases(listen.query, BEARER, &own).await;
    assert_query_cases(listen.query, SECOND_BEARER, &other).await;
    let missing = post_query(
        listen.query,
        Some(BEARER),
        &sql_body("SELECT log_region_string FROM logs"),
    )
    .await;
    assert_eq!(
        missing.status,
        reqwest::StatusCode::BAD_REQUEST,
        "{}",
        missing.body
    );
    assert_eq!(missing.body, r#"{"error":{"code":"invalid_sql"}}"#);
    let mut daemon = assert_cases_across_restart(&config_path, listen, daemon, BEARER, &own).await;
    assert_query_cases(listen.query, SECOND_BEARER, &other).await;
    daemon.terminate().await;
}

fn tenant_log(body: &str, host: &str, region: Option<&str>) -> ExportLogsServiceRequest {
    let mut attributes = vec![otlp::attribute("host", otlp::string_value(host))];
    if let Some(region) = region {
        attributes.push(otlp::attribute("region", otlp::string_value(region)));
    }
    otlp::logs(vec![otlp::resource(
        vec![],
        vec![otlp::scope(
            vec![],
            vec![LogRecord {
                time_unix_nano: HOUR_START,
                body: Some(otlp::string_value(body)),
                attributes,
                ..Default::default()
            }],
        )],
    )])
}

fn dynamic_request() -> ExportLogsServiceRequest {
    let first = LogRecord {
        time_unix_nano: HOUR_END,
        attributes: vec![
            otlp::attribute("host", otlp::string_value("one")),
            otlp::attribute("status", otlp::int_value(500)),
            otlp::attribute("host", otlp::string_value("two")),
            otlp::attribute("ok", otlp::bool_value(true)),
            otlp::attribute("latency", otlp::double_value(1.5)),
            otlp::attribute("nan", otlp::double_value(f64::NAN)),
            otlp::attribute("payload", otlp::bytes_value(b"hi")),
            otlp::attribute(
                "items",
                otlp::array_value(vec![otlp::int_value(1), otlp::string_value("x")]),
            ),
            otlp::attribute("http.status", otlp::int_value(1)),
            otlp::attribute(
                "http",
                otlp::kv_value(vec![otlp::attribute("status", otlp::int_value(2))]),
            ),
        ],
        ..Default::default()
    };
    let second = LogRecord {
        time_unix_nano: NEXT_HOUR,
        attributes: vec![
            otlp::attribute("status", otlp::string_value("ok")),
            otlp::attribute("region", otlp::string_value("eu")),
        ],
        ..Default::default()
    };
    let primary = otlp::resource(
        vec![
            otlp::attribute("service.name", otlp::string_value("api")),
            otlp::attribute("zone", otlp::string_value("eu")),
            otlp::attribute("service.name", otlp::string_value("edge")),
        ],
        vec![otlp::scope(
            vec![otlp::attribute("lib", otlp::string_value("probe"))],
            vec![first],
        )],
    );
    let secondary = otlp::resource(
        vec![otlp::attribute("service.name", otlp::int_value(5))],
        vec![otlp::scope(vec![], vec![second])],
    );
    otlp::logs(vec![primary, secondary])
}

fn later_column_request() -> ExportLogsServiceRequest {
    otlp::logs(vec![otlp::resource(
        vec![],
        vec![otlp::scope(
            vec![],
            vec![LogRecord {
                time_unix_nano: HOUR_START,
                attributes: vec![otlp::attribute("env", otlp::string_value("prod"))],
                ..Default::default()
            }],
        )],
    )])
}

fn collision_name(path: &[&str]) -> String {
    let mut hasher = blake3::Hasher::new();
    hasher.update(&[3]);
    hasher.update(&u64::try_from(path.len()).expect("path").to_be_bytes());
    for segment in path {
        hasher.update(&u64::try_from(segment.len()).expect("segment").to_be_bytes());
        hasher.update(segment.as_bytes());
    }
    hasher.update(&[2]);
    let hex = hasher.finalize().to_hex();
    format!("log_http_status_i64__{}", &hex[..8])
}

fn dynamic_schema(dotted: &str, nested: &str) -> Value {
    let mut schema = vec![
        json!({"name": "wal_sequence", "type": "UInt64", "nullable": false}),
        json!({"name": "record_index", "type": "UInt32", "nullable": false}),
        json!({"name": "service_name", "type": "Utf8", "nullable": true}),
        json!({"name": "resource_attributes", "type": "Utf8", "nullable": false}),
        json!({"name": "scope_attributes", "type": "Utf8", "nullable": false}),
        json!({"name": "log_attributes", "type": "Utf8", "nullable": false}),
        json!({"name": "resource_service_name_string", "type": "Utf8", "nullable": true}),
        json!({"name": "resource_service_name_i64", "type": "Int64", "nullable": true}),
        json!({"name": "scope_lib_string", "type": "Utf8", "nullable": true}),
        json!({"name": "log_host_string", "type": "Utf8", "nullable": true}),
        json!({"name": "log_ok_bool", "type": "Boolean", "nullable": true}),
        json!({"name": "log_status_i64", "type": "Int64", "nullable": true}),
        json!({"name": "log_status_string", "type": "Utf8", "nullable": true}),
        json!({"name": "log_latency_f64", "type": "Float64", "nullable": true}),
        json!({"name": "log_nan_f64", "type": "Float64", "nullable": true}),
        json!({"name": "log_payload_bytes", "type": "Binary", "nullable": true}),
        json!({"name": "log_items_json", "type": "Utf8", "nullable": true}),
        json!({"name": "log_region_string", "type": "Utf8", "nullable": true}),
        json!({"name": "log_env_string", "type": "Utf8", "nullable": true}),
    ];
    for name in [dotted, nested] {
        schema.push(json!({"name": name, "type": "Int64", "nullable": true}));
    }
    Value::Array(schema)
}

fn dynamic_rows(dotted: &str, nested: &str) -> Value {
    json!([
        {
            "wal_sequence": "0",
            "record_index": 0,
            "service_name": "edge",
            "resource_attributes": r#"{"service.name":"edge","zone":"eu"}"#,
            "scope_attributes": r#"{"lib":"probe"}"#,
            "log_attributes": r#"{"host":"two","http":{"status":2},"http.status":1,"items":[1,"x"],"latency":1.5,"nan":{"$float":"NaN"},"ok":true,"payload":{"$bytes":"aGk="},"status":500}"#,
            "resource_service_name_string": "edge",
            "resource_service_name_i64": null,
            "scope_lib_string": "probe",
            "log_host_string": "two",
            "log_ok_bool": true,
            "log_status_i64": "500",
            "log_status_string": null,
            "log_latency_f64": 1.5,
            "log_nan_f64": null,
            "log_payload_bytes": STANDARD.encode(b"hi"),
            "log_items_json": "[1,\"x\"]",
            "log_region_string": null,
            "log_env_string": null,
            dotted: "1",
            nested: "2"
        },
        {
            "wal_sequence": "0",
            "record_index": 1,
            "service_name": null,
            "resource_attributes": r#"{"service.name":5}"#,
            "scope_attributes": "{}",
            "log_attributes": r#"{"region":"eu","status":"ok"}"#,
            "resource_service_name_string": null,
            "resource_service_name_i64": "5",
            "scope_lib_string": null,
            "log_host_string": null,
            "log_ok_bool": null,
            "log_status_i64": null,
            "log_status_string": "ok",
            "log_latency_f64": null,
            "log_nan_f64": null,
            "log_payload_bytes": null,
            "log_items_json": null,
            "log_region_string": "eu",
            "log_env_string": null,
            dotted: null,
            nested: null
        },
        {
            "wal_sequence": "1",
            "record_index": 0,
            "service_name": null,
            "resource_attributes": "{}",
            "scope_attributes": "{}",
            "log_attributes": r#"{"env":"prod"}"#,
            "resource_service_name_string": null,
            "resource_service_name_i64": null,
            "scope_lib_string": null,
            "log_host_string": null,
            "log_ok_bool": null,
            "log_status_i64": null,
            "log_status_string": null,
            "log_latency_f64": null,
            "log_nan_f64": null,
            "log_payload_bytes": null,
            "log_items_json": null,
            "log_region_string": null,
            "log_env_string": "prod",
            dotted: null,
            nested: null
        }
    ])
}

async fn limited_dynamic_columns() {
    depth_fallback().await;
    column_cap().await;
}

async fn depth_fallback() {
    let (config_path, wal_directory, listen, daemon) = start_limited(StorageLimits {
        max_rows: 0,
        max_depth: 1,
        ..StorageLimits::default()
    })
    .await;
    post_logs(
        listen.http,
        &otlp::logs(vec![otlp::resource(
            vec![],
            vec![otlp::scope(
                vec![],
                vec![LogRecord {
                    time_unix_nano: HOUR_END,
                    attributes: vec![otlp::attribute(
                        "http",
                        otlp::kv_value(vec![
                            otlp::attribute("status", otlp::int_value(200)),
                            otlp::attribute(
                                "request",
                                otlp::kv_value(vec![otlp::attribute(
                                    "id",
                                    otlp::string_value("abc"),
                                )]),
                            ),
                        ]),
                    )],
                    ..Default::default()
                }],
            )],
        )]),
    )
    .await;
    wait_checkpoint(&wal_directory, TENANT, 1).await;
    let cases = [QueryCase {
        name: "nested map depth",
        sql: "SELECT log_http_status_i64, log_http_request_json, log_attributes FROM logs",
        schema: json!([
            {"name": "log_http_status_i64", "type": "Int64", "nullable": true},
            {"name": "log_http_request_json", "type": "Utf8", "nullable": true},
            {"name": "log_attributes", "type": "Utf8", "nullable": false}
        ]),
        rows: json!([{
            "log_http_status_i64": "200",
            "log_http_request_json": r#"{"id":"abc"}"#,
            "log_attributes": r#"{"http":{"request":{"id":"abc"},"status":200}}"#
        }]),
    }];
    let mut daemon =
        assert_cases_across_restart(&config_path, listen, daemon, BEARER, &cases).await;
    daemon.terminate().await;
}

async fn column_cap() {
    let (config_path, wal_directory, listen, daemon) = start_limited(StorageLimits {
        max_rows: 0,
        max_dynamic_columns: 1,
        ..StorageLimits::default()
    })
    .await;
    post_logs(
        listen.http,
        &otlp::logs(vec![otlp::resource(
            vec![],
            vec![otlp::scope(
                vec![],
                vec![LogRecord {
                    time_unix_nano: HOUR_END,
                    attributes: vec![
                        otlp::attribute("c", otlp::int_value(3)),
                        otlp::attribute("a", otlp::int_value(1)),
                        otlp::attribute("b", otlp::int_value(2)),
                    ],
                    ..Default::default()
                }],
            )],
        )]),
    )
    .await;
    wait_checkpoint(&wal_directory, TENANT, 1).await;
    let missing = post_query(
        listen.query,
        Some(BEARER),
        &sql_body("SELECT log_b_i64 FROM logs"),
    )
    .await;
    assert_eq!(
        missing.status,
        reqwest::StatusCode::BAD_REQUEST,
        "{}",
        missing.body
    );
    assert_eq!(missing.body, r#"{"error":{"code":"invalid_sql"}}"#);
    let cases = [QueryCase {
        name: "column cap",
        sql: "SELECT log_a_i64, log_attributes FROM logs",
        schema: json!([
            {"name": "log_a_i64", "type": "Int64", "nullable": true},
            {"name": "log_attributes", "type": "Utf8", "nullable": false}
        ]),
        rows: json!([{
            "log_a_i64": "1",
            "log_attributes": r#"{"a":1,"b":2,"c":3}"#
        }]),
    }];
    let mut daemon =
        assert_cases_across_restart(&config_path, listen, daemon, BEARER, &cases).await;
    let missing_after_restart = post_query(
        listen.query,
        Some(BEARER),
        &sql_body("SELECT log_c_i64 FROM logs"),
    )
    .await;
    assert_eq!(
        missing_after_restart.status,
        reqwest::StatusCode::BAD_REQUEST
    );
    daemon.terminate().await;
}

async fn start_limited(
    limits: StorageLimits,
) -> (
    std::path::PathBuf,
    std::path::PathBuf,
    support::ListenAddrs,
    support::Observerd,
) {
    let root = tempfile::tempdir().expect("tempdir").keep();
    let wal_directory = root.join("wal");
    let config_path = root.join("observerd.toml");
    let listen = reserve_ports();
    write_storage(&config_path, &wal_directory, listen, &limits);
    let daemon = start_ready(&config_path, listen).await;
    (config_path, wal_directory, listen, daemon)
}

#[tokio::test]
async fn sql_filters_and_aggregates_keep_exact_json_values() {
    timeout(TEST_TIMEOUT * 3, sql_over_ingested_logs())
        .await
        .expect("sql round trip timed out");
}

async fn sql_over_ingested_logs() {
    let root = tempfile::tempdir().expect("tempdir");
    let (listen, mut daemon) = start_daemon(
        &root.path().join("observerd.toml"),
        &root.path().join("wal"),
    )
    .await;
    post_logs(listen.http, &sql_corpus()).await;
    assert_query_cases(listen.query, BEARER, &sql_cases()).await;
    daemon.terminate().await;
}

fn sql_corpus() -> ExportLogsServiceRequest {
    otlp::logs(vec![otlp::resource(
        vec![],
        vec![otlp::scope(
            vec![],
            vec![
                sql_row(
                    "alpha",
                    HOUR_START,
                    Some(10),
                    Some(1.5),
                    Some(b"hi"),
                    Some("web"),
                ),
                sql_row("beta", HOUR_END, Some(30), Some(2.5), None, None),
                sql_row("gamma", NEXT_HOUR, None, None, None, Some("api")),
            ],
        )],
    )])
}

fn sql_row(
    body: &str,
    time_unix_nano: u64,
    status: Option<i64>,
    latency: Option<f64>,
    payload: Option<&[u8]>,
    host: Option<&str>,
) -> LogRecord {
    let mut attributes = Vec::new();
    if let Some(status) = status {
        attributes.push(otlp::attribute("status", otlp::int_value(status)));
    }
    if let Some(latency) = latency {
        attributes.push(otlp::attribute("latency", otlp::double_value(latency)));
    }
    if let Some(payload) = payload {
        attributes.push(otlp::attribute("payload", otlp::bytes_value(payload)));
    }
    if let Some(host) = host {
        attributes.push(otlp::attribute("host", otlp::string_value(host)));
    }
    LogRecord {
        time_unix_nano,
        body: Some(otlp::string_value(body)),
        attributes,
        ..Default::default()
    }
}

fn sql_cases() -> [QueryCase; 10] {
    let bytes = STANDARD.encode(b"hi");
    [
        QueryCase {
            name: "time bound",
            sql: "SELECT body FROM logs WHERE event_time_unix_nano >= 1700002799999999999 ORDER BY wal_sequence",
            schema: json!([{"name": "body", "type": "Utf8", "nullable": true}]),
            rows: json!([{"body": "beta"}, {"body": "gamma"}]),
        },
        QueryCase {
            name: "body predicate",
            sql: "SELECT wal_sequence FROM logs WHERE body = 'alpha'",
            schema: json!([{"name": "wal_sequence", "type": "UInt64", "nullable": false}]),
            rows: json!([{"wal_sequence": "0"}]),
        },
        QueryCase {
            name: "typed dynamic filter",
            sql: "SELECT body, log_status_i64, log_latency_f64, log_payload_bytes FROM logs WHERE log_status_i64 = 10",
            schema: json!([
                {"name": "body", "type": "Utf8", "nullable": true},
                {"name": "log_status_i64", "type": "Int64", "nullable": true},
                {"name": "log_latency_f64", "type": "Float64", "nullable": true},
                {"name": "log_payload_bytes", "type": "Binary", "nullable": true}
            ]),
            rows: json!([{
                "body": "alpha",
                "log_status_i64": "10",
                "log_latency_f64": 1.5,
                "log_payload_bytes": bytes
            }]),
        },
        QueryCase {
            name: "null predicate",
            sql: "SELECT body FROM logs WHERE log_host_string IS NULL ORDER BY wal_sequence",
            schema: json!([{"name": "body", "type": "Utf8", "nullable": true}]),
            rows: json!([{"body": "beta"}]),
        },
        QueryCase {
            name: "alias order and limit",
            sql: "SELECT body AS message FROM logs ORDER BY event_time_unix_nano DESC LIMIT 1",
            schema: json!([{"name": "message", "type": "Utf8", "nullable": true}]),
            rows: json!([{"message": "gamma"}]),
        },
        QueryCase {
            name: "count",
            sql: "SELECT count(*) AS rows_counted FROM logs",
            schema: json!([{"name": "rows_counted", "type": "Int64", "nullable": false}]),
            rows: json!([{"rows_counted": "3"}]),
        },
        QueryCase {
            name: "grouped count",
            sql: "SELECT body, count(*) AS rows_counted FROM logs GROUP BY body ORDER BY body",
            schema: json!([
                {"name": "body", "type": "Utf8", "nullable": true},
                {"name": "rows_counted", "type": "Int64", "nullable": false}
            ]),
            rows: json!([
                {"body": "alpha", "rows_counted": "1"},
                {"body": "beta", "rows_counted": "1"},
                {"body": "gamma", "rows_counted": "1"}
            ]),
        },
        QueryCase {
            name: "sum",
            sql: "SELECT sum(log_status_i64) AS total FROM logs",
            schema: json!([{"name": "total", "type": "Int64", "nullable": true}]),
            rows: json!([{"total": "40"}]),
        },
        QueryCase {
            name: "average",
            sql: "SELECT avg(log_latency_f64) AS mean FROM logs",
            schema: json!([{"name": "mean", "type": "Float64", "nullable": true}]),
            rows: json!([{"mean": 2}]),
        },
        QueryCase {
            name: "empty filter",
            sql: "SELECT body, event_time_unix_nano FROM logs WHERE body = 'missing'",
            schema: json!([
                {"name": "body", "type": "Utf8", "nullable": true},
                {"name": "event_time_unix_nano", "type": "UInt64", "nullable": false}
            ]),
            rows: json!([]),
        },
    ]
}
