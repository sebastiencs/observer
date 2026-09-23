#![allow(dead_code)]

pub mod otlp;

use std::{
    fs,
    net::{SocketAddr, TcpListener},
    path::Path,
    process::Stdio,
    time::{Duration, Instant},
};

use nix::{
    sys::signal::{Signal, kill},
    unistd::Pid,
};
use observer_protocol::otlp::{
    AnyValue, ExportLogsServiceRequest, ExportLogsServiceResponse, KeyValue, Resource,
    ResourceLogs, any_value,
};
use observer_wal::{Frame, WalReader, tenant_wal_directory};
use prost::Message;
use reqwest::StatusCode;
use serde_json::Value;
use tokio::{
    process::{Child, Command},
    time::timeout,
};

pub const TEST_TIMEOUT: Duration = Duration::from_secs(10);
pub const START_ATTEMPTS: usize = 3;
pub const SECRET: &str = "secret-a";
pub const TENANT: &str = "tenant-a";
pub const BEARER: &str = "Bearer secret-a";
pub const SECOND_SECRET: &str = "secret-b";
pub const SECOND_TENANT: &str = "tenant-b";
pub const SECOND_BEARER: &str = "Bearer secret-b";

#[derive(Clone, Copy, Debug)]
pub struct ListenAddrs {
    pub grpc: SocketAddr,
    pub http: SocketAddr,
    pub admin: SocketAddr,
    pub query: SocketAddr,
}

pub fn reserve_ports() -> ListenAddrs {
    let grpc = TcpListener::bind("127.0.0.1:0").expect("reserve grpc");
    let http = TcpListener::bind("127.0.0.1:0").expect("reserve http");
    let admin = TcpListener::bind("127.0.0.1:0").expect("reserve admin");
    let query = TcpListener::bind("127.0.0.1:0").expect("reserve query");
    let addrs = ListenAddrs {
        grpc: grpc.local_addr().expect("grpc addr"),
        http: http.local_addr().expect("http addr"),
        admin: admin.local_addr().expect("admin addr"),
        query: query.local_addr().expect("query addr"),
    };
    drop((grpc, http, admin, query));
    addrs
}

pub fn data_directory(wal_directory: &Path) -> std::path::PathBuf {
    wal_directory.parent().unwrap_or(wal_directory).join("data")
}

/// Memtable and readiness limits written into a daemon config.
#[derive(Clone, Debug)]
pub struct StorageLimits {
    pub max_rows: u64,
    pub max_bytes: u64,
    pub max_age_ms: u64,
    pub max_frozen: usize,
    pub max_dynamic_columns: usize,
    pub max_depth: usize,
    pub poll_interval_ms: u64,
    pub min_free_bytes: u64,
}

impl Default for StorageLimits {
    fn default() -> Self {
        Self {
            max_rows: 100_000,
            max_bytes: 67_108_864,
            max_age_ms: 60_000,
            max_frozen: 4,
            max_dynamic_columns: 256,
            max_depth: 4,
            poll_interval_ms: 20,
            min_free_bytes: 1,
        }
    }
}

pub fn write_config(path: &Path, wal_directory: &Path, listen: ListenAddrs) {
    write_storage(path, wal_directory, listen, &StorageLimits::default());
}

pub fn write_tuned(
    path: &Path,
    wal_directory: &Path,
    listen: ListenAddrs,
    max_rows: u64,
    min_free_bytes: u64,
) {
    write_storage(
        path,
        wal_directory,
        listen,
        &StorageLimits {
            max_rows,
            min_free_bytes,
            ..StorageLimits::default()
        },
    );
}

pub fn write_storage(
    path: &Path,
    wal_directory: &Path,
    listen: ListenAddrs,
    limits: &StorageLimits,
) {
    write_daemon(path, wal_directory, listen, limits, None);
}

/// Daemon config that publishes each row and rejects a normal query response.
pub fn write_response_cap(
    path: &Path,
    wal_directory: &Path,
    listen: ListenAddrs,
    max_response_bytes: u64,
) {
    write_daemon(
        path,
        wal_directory,
        listen,
        &StorageLimits {
            max_rows: 0,
            ..StorageLimits::default()
        },
        Some(max_response_bytes),
    );
}

pub fn write_daemon(
    path: &Path,
    wal_directory: &Path,
    listen: ListenAddrs,
    limits: &StorageLimits,
    max_response_bytes: Option<u64>,
) {
    let mut contents = config_contents(wal_directory, listen, limits);
    if let Some(max_response_bytes) = max_response_bytes {
        contents.push_str(&format!(
            "\n[query]\nmax_response_bytes = {max_response_bytes}\n"
        ));
    }
    fs::write(path, contents).expect("write config");
}

fn config_contents(wal_directory: &Path, listen: ListenAddrs, limits: &StorageLimits) -> String {
    let data_directory = data_directory(wal_directory);
    format!(
        "wal_directory = {wal_directory:?}\ndata_directory = {data_directory:?}\n\n[listen]\ngrpc = \"{grpc}\"\nhttp = \"{http}\"\nadmin = \"{admin}\"\nquery = \"{query}\"\n\n[tokens]\n\"{SECRET}\" = \"{TENANT}\"\n\"{SECOND_SECRET}\" = \"{SECOND_TENANT}\"\n\n[storage]\nmax_rows = {max_rows}\nmax_bytes = {max_bytes}\nmax_age_ms = {max_age_ms}\nmax_frozen = {max_frozen}\nmax_dynamic_columns = {max_dynamic_columns}\nmax_depth = {max_depth}\npoll_interval_ms = {poll_interval_ms}\n\n[readiness]\nmin_free_bytes = {min_free_bytes}\n",
        grpc = listen.grpc,
        http = listen.http,
        admin = listen.admin,
        query = listen.query,
        max_rows = limits.max_rows,
        max_bytes = limits.max_bytes,
        max_age_ms = limits.max_age_ms,
        max_frozen = limits.max_frozen,
        max_dynamic_columns = limits.max_dynamic_columns,
        max_depth = limits.max_depth,
        poll_interval_ms = limits.poll_interval_ms,
        min_free_bytes = limits.min_free_bytes,
    )
}

pub struct Observerd {
    child: Child,
    stderr: Option<tokio::process::ChildStderr>,
}

impl Observerd {
    pub fn spawn(config: &Path) -> Self {
        let mut child = Command::new(env!("CARGO_BIN_EXE_observerd"))
            .arg(config)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .expect("spawn observerd");
        let stderr = child.stderr.take();
        Self { child, stderr }
    }

    pub async fn wait_ready(&mut self, admin: SocketAddr) -> Result<(), String> {
        let client = reqwest::Client::new();
        let url = format!("http://{admin}/ready");
        let deadline = Instant::now() + TEST_TIMEOUT;
        loop {
            if Instant::now() >= deadline {
                return Err(self.exit_context("timed out waiting for /ready").await);
            }
            if let Some(status) = self.child.try_wait().expect("try_wait") {
                return Err(self
                    .exit_context(format!("exited before ready ({status})"))
                    .await);
            }
            if let Ok(response) = client.get(&url).send().await
                && response.status() == StatusCode::OK
            {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    pub async fn wait_exit(&mut self) -> std::process::ExitStatus {
        match timeout(TEST_TIMEOUT, self.child.wait()).await {
            Ok(Ok(status)) => status,
            Ok(Err(error)) => panic!(
                "{}",
                self.exit_context(format!("wait failed: {error}")).await
            ),
            Err(_) => panic!("{}", self.exit_context("timed out waiting for exit").await),
        }
    }

    pub async fn terminate(&mut self) {
        let pid = self.child.id().expect("child pid");
        kill(
            Pid::from_raw(i32::try_from(pid).expect("pid fits i32")),
            Signal::SIGTERM,
        )
        .expect("send SIGTERM");
        let status = timeout(TEST_TIMEOUT, self.child.wait())
            .await
            .expect("timed out waiting for observerd to exit")
            .expect("wait for observerd");
        if !status.success() {
            panic!(
                "{}",
                self.exit_context(format!("observerd exited unsuccessfully ({status})"))
                    .await
            );
        }
    }

    async fn exit_context(&mut self, prefix: impl Into<String>) -> String {
        let mut message = prefix.into();
        if let Some(mut stderr) = self.stderr.take() {
            let mut bytes = Vec::new();
            let _ = tokio::io::AsyncReadExt::read_to_end(&mut stderr, &mut bytes).await;
            if !bytes.is_empty() {
                message.push_str("\nstderr:\n");
                message.push_str(&String::from_utf8_lossy(&bytes));
            }
        }
        message
    }
}

pub async fn start_daemon(config_path: &Path, wal_directory: &Path) -> (ListenAddrs, Observerd) {
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

pub async fn restart_daemon(config_path: &Path, listen: ListenAddrs) -> Observerd {
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

pub async fn start_ready(config: &Path, listen: ListenAddrs) -> Observerd {
    let mut daemon = Observerd::spawn(config);
    daemon.wait_ready(listen.admin).await.expect("ready");
    daemon
}

pub fn logs_request(marker: &str) -> ExportLogsServiceRequest {
    ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            resource: Some(Resource {
                attributes: vec![KeyValue {
                    key: "e2e.marker".to_owned(),
                    value: Some(AnyValue {
                        value: Some(any_value::Value::StringValue(marker.to_owned())),
                    }),
                    ..Default::default()
                }],
                ..Default::default()
            }),
            ..Default::default()
        }],
    }
}

pub struct QueryHttpResponse {
    pub status: StatusCode,
    pub headers: reqwest::header::HeaderMap,
    pub body: String,
}

impl QueryHttpResponse {
    pub fn json(&self) -> serde_json::Value {
        serde_json::from_str(&self.body)
            .unwrap_or_else(|error| panic!("query response is not json ({error}): {}", self.body))
    }
}

pub async fn post_query(
    address: SocketAddr,
    bearer: Option<&str>,
    body: &str,
) -> QueryHttpResponse {
    let client = reqwest::Client::new();
    let mut request = client
        .post(format!("http://{address}/v1/query"))
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .header(reqwest::header::ORIGIN, "https://app.example")
        .body(body.to_owned());
    if let Some(bearer) = bearer {
        request = request.header(reqwest::header::AUTHORIZATION, bearer);
    }
    let response = request.send().await.expect("post /v1/query");
    let status = response.status();
    let headers = response.headers().clone();
    let body = response.text().await.expect("query body");
    QueryHttpResponse {
        status,
        headers,
        body,
    }
}

pub struct HttpBytes {
    pub status: StatusCode,
    pub headers: reqwest::header::HeaderMap,
    pub body: Vec<u8>,
}

impl HttpBytes {
    pub fn text(&self) -> String {
        String::from_utf8_lossy(&self.body).into_owned()
    }
}

pub async fn post_logs_raw(
    address: SocketAddr,
    bearer: Option<&str>,
    content_type: Option<&str>,
    body: impl Into<Vec<u8>>,
) -> HttpBytes {
    let client = reqwest::Client::new();
    let mut request = client
        .post(format!("http://{address}/v1/logs"))
        .body(body.into());
    if let Some(content_type) = content_type {
        request = request.header(reqwest::header::CONTENT_TYPE, content_type);
    }
    if let Some(bearer) = bearer {
        request = request.header(reqwest::header::AUTHORIZATION, bearer);
    }
    let response = request.send().await.expect("post /v1/logs");
    let status = response.status();
    let headers = response.headers().clone();
    let body = response.bytes().await.expect("logs body").to_vec();
    HttpBytes {
        status,
        headers,
        body,
    }
}

pub async fn post_logs(http: SocketAddr, request: &ExportLogsServiceRequest) {
    post_logs_as(http, BEARER, request).await;
}

pub async fn post_logs_as(http: SocketAddr, bearer: &str, request: &ExportLogsServiceRequest) {
    let response = reqwest::Client::new()
        .post(format!("http://{http}/v1/logs"))
        .header(reqwest::header::CONTENT_TYPE, "application/x-protobuf")
        .header(reqwest::header::AUTHORIZATION, bearer)
        .body(request.encode_to_vec())
        .send()
        .await
        .expect("post /v1/logs");
    assert_eq!(
        response.status(),
        StatusCode::OK,
        "export failed: {}",
        response.text().await.unwrap_or_default()
    );
    let decoded = ExportLogsServiceResponse::decode(response.bytes().await.expect("body"))
        .expect("decode export response");
    assert!(decoded.partial_success.is_none());
}

/// One named SQL projection and the schema and rows it must return.
pub struct QueryCase {
    pub name: &'static str,
    pub sql: &'static str,
    pub schema: Value,
    pub rows: Value,
}

pub fn sql_body(sql: &str) -> String {
    serde_json::json!({ "sql": sql }).to_string()
}

pub fn column_strings(document: &Value, column: &str) -> Vec<String> {
    document["rows"]
        .as_array()
        .expect("rows")
        .iter()
        .map(|row| {
            row[column]
                .as_str()
                .unwrap_or_else(|| panic!("{column} is not a string in {row}"))
                .to_owned()
        })
        .collect()
}

pub fn assert_metrics(metrics: &Value, rows: u64) {
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

pub async fn poll_query(
    address: SocketAddr,
    bearer: &str,
    sql: &str,
    mut ready: impl FnMut(&Value) -> bool,
) -> Value {
    let deadline = Instant::now() + TEST_TIMEOUT;
    let body = sql_body(sql);
    let mut last = String::new();
    while Instant::now() < deadline {
        let response = post_query(address, Some(bearer), &body).await;
        last = response.body;
        if response.status == StatusCode::OK
            && let Ok(document) = serde_json::from_str::<Value>(&last)
            && ready(&document)
        {
            return document;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("query did not match\nsql: {sql}\nlast: {last}");
}

pub async fn wait_for_bodies(address: SocketAddr, bearer: &str, expected: &[&str]) {
    let expected = expected
        .iter()
        .map(|body| (*body).to_owned())
        .collect::<Vec<_>>();
    poll_query(
        address,
        bearer,
        "SELECT body FROM logs ORDER BY body",
        |document| column_strings(document, "body") == expected,
    )
    .await;
}

pub async fn assert_query_cases(address: SocketAddr, bearer: &str, cases: &[QueryCase]) {
    for case in cases {
        assert_query_case(address, bearer, case).await;
    }
}

pub async fn assert_query_case(address: SocketAddr, bearer: &str, case: &QueryCase) {
    let deadline = Instant::now() + TEST_TIMEOUT;
    let body = sql_body(case.sql);
    let mut last = String::new();
    while Instant::now() < deadline {
        let response = post_query(address, Some(bearer), &body).await;
        last = response.body;
        if response.status == StatusCode::OK
            && let Ok(document) = serde_json::from_str::<Value>(&last)
            && document["schema"] == case.schema
            && document["rows"] == case.rows
        {
            let count = case.rows.as_array().expect("expected rows").len();
            assert_metrics(
                &document["metrics"],
                u64::try_from(count).expect("row count"),
            );
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!(
        "case {} did not match\nsql: {}\nexpected schema: {}\nexpected rows: {}\nlast: {last}",
        case.name, case.sql, case.schema, case.rows
    );
}

/// Check `cases` against the live daemon, publish through shutdown, and check them again.
pub async fn assert_cases_across_restart(
    config_path: &Path,
    listen: ListenAddrs,
    mut daemon: Observerd,
    bearer: &str,
    cases: &[QueryCase],
) -> Observerd {
    assert_query_cases(listen.query, bearer, cases).await;
    daemon.terminate().await;
    let daemon = restart_daemon(config_path, listen).await;
    assert_query_cases(listen.query, bearer, cases).await;
    daemon
}

pub async fn wait_checkpoint(wal_directory: &Path, tenant: &str, sequence: u64) {
    let directory = observer_wal::tenant_wal_directory(wal_directory, tenant);
    let start = Instant::now();
    while start.elapsed() < TEST_TIMEOUT {
        if observer_wal::WalCheckpoint::load(&directory)
            .ok()
            .is_some_and(|checkpoint| checkpoint.cursor().next_sequence() == sequence)
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("checkpoint for {tenant} did not reach {sequence}");
}

pub fn frames_on_disk(wal_directory: &Path, tenant_id: &str) -> Vec<Frame> {
    let tenant_directory = tenant_wal_directory(wal_directory, tenant_id);
    let mut reader = WalReader::open(&tenant_directory).unwrap_or_else(|error| {
        panic!("open WAL reader {}: {error}", tenant_directory.display());
    });
    let mut frames = Vec::new();
    while let Some(record) = reader.next_record().unwrap_or_else(|error| {
        panic!("read WAL {}: {error}", wal_directory.display());
    }) {
        frames.push(record.frame);
    }
    frames
}
