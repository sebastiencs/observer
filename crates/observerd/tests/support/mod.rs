use std::{
    fs,
    net::{SocketAddr, TcpListener},
    path::{Path, PathBuf},
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
use observer_wal::{Frame, SEGMENT_HEADER_SIZE, decode};
use prost::Message;
use reqwest::StatusCode;
use tokio::{
    process::{Child, Command},
    time::timeout,
};

pub const TEST_TIMEOUT: Duration = Duration::from_secs(10);
pub const SECRET: &str = "secret-a";
pub const TENANT: &str = "tenant-a";
pub const BEARER: &str = "Bearer secret-a";

#[derive(Clone, Copy, Debug)]
pub struct ListenAddrs {
    pub grpc: SocketAddr,
    pub http: SocketAddr,
    pub admin: SocketAddr,
}

pub fn reserve_ports() -> ListenAddrs {
    let grpc = TcpListener::bind("127.0.0.1:0").expect("reserve grpc");
    let http = TcpListener::bind("127.0.0.1:0").expect("reserve http");
    let admin = TcpListener::bind("127.0.0.1:0").expect("reserve admin");
    let addrs = ListenAddrs {
        grpc: grpc.local_addr().expect("grpc addr"),
        http: http.local_addr().expect("http addr"),
        admin: admin.local_addr().expect("admin addr"),
    };
    drop((grpc, http, admin));
    addrs
}

pub fn write_config(path: &Path, wal_directory: &Path, listen: ListenAddrs) {
    let contents = format!(
        "wal_directory = {wal_directory:?}\n\n[listen]\ngrpc = \"{grpc}\"\nhttp = \"{http}\"\nadmin = \"{admin}\"\n\n[tokens]\n\"{SECRET}\" = \"{TENANT}\"\n",
        grpc = listen.grpc,
        http = listen.http,
        admin = listen.admin,
    );
    fs::write(path, contents).expect("write config");
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

pub async fn post_logs(http: SocketAddr, request: &ExportLogsServiceRequest) {
    let response = reqwest::Client::new()
        .post(format!("http://{http}/v1/logs"))
        .header(reqwest::header::CONTENT_TYPE, "application/x-protobuf")
        .header(reqwest::header::AUTHORIZATION, BEARER)
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

pub fn frames_on_disk(wal_directory: &Path) -> Vec<Frame> {
    let lane = wal_directory.join("lane-0000");
    let mut paths: Vec<PathBuf> = fs::read_dir(&lane)
        .unwrap_or_else(|error| panic!("read {}: {error}", lane.display()))
        .map(|entry| entry.expect("dir entry").path())
        .filter(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.ends_with(".open") || name.ends_with(".wal"))
        })
        .collect();
    paths.sort();

    let mut frames = Vec::new();
    for path in paths {
        let bytes = fs::read(&path).unwrap_or_else(|error| {
            panic!("read {}: {error}", path.display());
        });
        let mut offset = SEGMENT_HEADER_SIZE;
        while offset < bytes.len() {
            let (frame, consumed) = decode(&bytes[offset..]).unwrap_or_else(|error| {
                panic!("decode {} at {offset}: {error}", path.display());
            });
            frames.push(frame);
            offset += consumed;
        }
    }
    frames
}
